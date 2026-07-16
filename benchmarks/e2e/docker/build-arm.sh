#!/usr/bin/env bash
#
# Benchmark v2 — build one arm's Kafka Connect image (plan phase 2).
#
# Usage:
#   build-arm.sh --arm head|pinned [--image-tag <full ref>] [--no-docker]
#
#   --arm        head   : build the plugin from the CURRENT working tree (the sha
#                         under test). This is what the H arm benchmarks.
#                pinned : download the RELEASED plugin artifact for the pinned tag
#                         (read from PINNED_REF) from its GitHub release and bake
#                         THAT immutable jar in — never rebuild it. Rationale: a
#                         rebuilt pinned plugin would drift with toolchain changes
#                         (gradle/JDK/deps), so the H/P ratio could move for reasons
#                         that are not connector changes. The released zip is the
#                         fixed reference point; drift in the (nightly-rebuilt) base
#                         image layers hits both arms and cancels, but the plugin
#                         must not. The download is verified against the release
#                         asset's sha256 digest and that sha256 is recorded in the
#                         image provenance.
#   --image-tag  full image ref to tag/build (default: local
#                clickhouse-kafka-connect-benchmark:<arm>-<sha|version>).
#   --push       after `docker build`, `docker push` the tag and resolve the
#                registry DIGEST. Digest-pinning is the benchmark default (a
#                mutable tag is served STALE by node/registry caches — the
#                stale-tag failure class): callers must consume the digest ref
#                (repo@sha256:...), never the tag, after a push. Without --push
#                the digest cannot be resolved (a local image has no RepoDigest),
#                so the emitted image_digest_ref is empty and a machine-readable
#                warning is printed.
#   --digest-out write the resolved digest ref (repo@sha256:...) to this file
#                (single line, no trailing marker) so a caller can capture it
#                without parsing stdout. Requires --push.
#   --no-docker  build the plugin zip and print the provenance/build args only;
#                skip `docker build` (used when docker is unavailable).
#
# Env overrides (aligned with task 25's Strimzi/Kafka pin; see Dockerfile default):
#   STRIMZI_IMAGE, STRIMZI_VERSION, KAFKA_VERSION.
#
# Emits, on success, KEY=VALUE lines on stdout (image_ref, git_sha, arm,
# connector_version, strimzi/kafka versions, plugin_zip) for the caller/orchestrator.
# After a --push, ALSO emits the machine-readable digest line
#   IMAGE_DIGEST=<repo>@sha256:...
# plus image_digest_ref=<same> — callers must pin on THIS, not the tag.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PINNED_REF_FILE="${SCRIPT_DIR}/PINNED_REF"

# Strimzi/Kafka pin. Keep in sync with task 25's deployed Strimzi operator
# version (benchmarks/e2e/infra/env.sh: STRIMZI_VERSION); the base image default
# also lives in the Dockerfile. Overridable via env.
STRIMZI_IMAGE="${STRIMZI_IMAGE:-quay.io/strimzi/kafka:0.46.0-kafka-3.9.0}"
STRIMZI_VERSION="${STRIMZI_VERSION:-0.46.0}"
KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"

# Confluent Avro converter pin (task 31 review F2). The benchmark's insert path
# is Avro + Schema Registry via io.confluent.connect.avro.AvroConverter (plan
# §6), but that class ships in NEITHER the stock Strimzi base image NOR the
# connector plugin zip — without baking it in, the worker throws
# ClassNotFoundException on the first connector deploy. We bake the Confluent
# Hub archive (a self-contained plugin dir: converter + avro +
# schema-registry-client + all transitive deps) into BOTH arm images; both arms
# get the SAME pinned converter, so it cancels in the H/P ratio exactly like
# the shared base-image layers. Version pinned to match the deployed Schema
# Registry (infra/schema-registry.yaml: cp-schema-registry:7.7.1). The sha256
# below was computed from the Confluent Hub download at pin time and every
# build verifies against it (a mismatch FAILS the build). Bump version+sha
# together, deliberately.
AVRO_CONVERTER_VERSION="${AVRO_CONVERTER_VERSION:-7.7.1}"
AVRO_CONVERTER_SHA256="${AVRO_CONVERTER_SHA256:-f62604f1da08c143ac9a09dc2a5c53c12f9969c8fc96a77d355626c7878faa95}"
AVRO_CONVERTER_URL="https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip"

ARM=""
IMAGE_TAG=""
RUN_DOCKER=1
DO_PUSH=0
DIGEST_OUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arm) ARM="$2"; shift 2 ;;
    --image-tag) IMAGE_TAG="$2"; shift 2 ;;
    --push) DO_PUSH=1; shift ;;
    --digest-out) DIGEST_OUT="$2"; shift 2 ;;
    --no-docker) RUN_DOCKER=0; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -n "${DIGEST_OUT}" && "${DO_PUSH}" -ne 1 ]]; then
  echo "error: --digest-out requires --push (a local image has no registry digest)" >&2
  exit 2
fi

if [[ "${ARM}" != "head" && "${ARM}" != "pinned" ]]; then
  echo "error: --arm must be 'head' or 'pinned'" >&2
  exit 2
fi

read_pinned_ref() {
  # First non-comment, non-blank line of PINNED_REF.
  grep -v '^[[:space:]]*#' "${PINNED_REF_FILE}" | grep -v '^[[:space:]]*$' | head -1 | tr -d '[:space:]'
}

# The gradle task that produces the Confluent-archive plugin zip
# (build/confluent/clickhouse-kafka-connect-<version>.zip). Verified locally.
GRADLE_ARCHIVE_TASK="createConfluentArchive"

# sha256 of the plugin zip that gets baked in (recorded in provenance). For the
# pinned arm this is the release asset's published digest (verified); for head it
# is computed from the freshly built zip.
PLUGIN_SHA256=""

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# HEAD arm: build the plugin zip from the current working tree.
# Sets SRC_DIR, GIT_SHA, CONNECTOR_VERSION, PLUGIN_ZIP, PLUGIN_SHA256.
build_head_plugin() {
  SRC_DIR="${REPO_ROOT}"
  GIT_SHA="$(git -C "${SRC_DIR}" rev-parse HEAD)"
  CONNECTOR_VERSION="$(tr -d '[:space:]' < "${SRC_DIR}/VERSION")"

  echo ">> building head plugin (sha=${GIT_SHA}, version=${CONNECTOR_VERSION}) via ${GRADLE_ARCHIVE_TASK}" >&2
  ( cd "${SRC_DIR}" && ./gradlew "${GRADLE_ARCHIVE_TASK}" -x test ) >&2

  PLUGIN_ZIP="${SRC_DIR}/build/confluent/clickhouse-kafka-connect-${CONNECTOR_VERSION}.zip"
  if [[ ! -f "${PLUGIN_ZIP}" ]]; then
    PLUGIN_ZIP="$(ls -1 "${SRC_DIR}"/build/confluent/*.zip 2>/dev/null | head -1 || true)"
  fi
  if [[ ! -f "${PLUGIN_ZIP}" ]]; then
    echo "error: plugin zip not found under ${SRC_DIR}/build/confluent/" >&2; exit 1
  fi
  PLUGIN_SHA256="$(sha256_of "${PLUGIN_ZIP}")"
}

# PINNED arm: download the immutable released artifact for the pinned tag, verify
# its sha256 against the release asset's published digest, and bake THAT in.
# Never rebuilds. Sets GIT_SHA (the tag's commit), CONNECTOR_VERSION, PLUGIN_ZIP,
# PLUGIN_SHA256.
fetch_pinned_plugin() {
  local ref
  ref="$(read_pinned_ref)"
  if [[ -z "${ref}" ]]; then
    echo "error: PINNED_REF empty" >&2; exit 1
  fi
  CONNECTOR_VERSION="${ref}"

  local dl_dir
  dl_dir="$(mktemp -d "${TMPDIR:-/tmp}/ckc-pinned-${ref}.XXXXXX")"
  trap 'rm -rf "'"${dl_dir}"'"' EXIT

  # The tag's commit sha for provenance. rev-parse works if tags are present;
  # otherwise fall back to the GitHub API.
  GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse -q "refs/tags/${ref}^{commit}" 2>/dev/null || true)"

  echo ">> fetching released plugin artifact for pinned tag ${ref}" >&2
  # Prefer gh (uses GITHUB_TOKEN in CI); capture the published sha256 digest.
  local expected_sha=""
  if command -v gh >/dev/null 2>&1; then
    local asset_name digest
    # Single release asset per the release convention; pick the .zip.
    asset_name="$(gh release view "${ref}" --repo ClickHouse/clickhouse-kafka-connect \
      --json assets --jq '.assets[] | select(.name|endswith(".zip")) | .name' | head -1)"
    if [[ -z "${asset_name}" ]]; then
      echo "error: no .zip asset on release ${ref}" >&2; exit 1
    fi
    digest="$(gh release view "${ref}" --repo ClickHouse/clickhouse-kafka-connect \
      --json assets --jq ".assets[] | select(.name==\"${asset_name}\") | .digest" | head -1)"
    # A release with no published digest makes jq emit the literal "null" —
    # treat that as absent so we take the warn-and-record path, not a mismatch.
    # (if-form, not `[[ ]] &&`: a false test would trip set -e.)
    if [[ "${digest}" == "null" ]]; then digest=""; fi
    expected_sha="${digest#sha256:}"
    if [[ -z "${GIT_SHA}" ]]; then
      GIT_SHA="$(gh api "repos/ClickHouse/clickhouse-kafka-connect/git/refs/tags/${ref}" \
        --jq '.object.sha' 2>/dev/null || echo unknown)"
    fi
    gh release download "${ref}" --repo ClickHouse/clickhouse-kafka-connect \
      --pattern "${asset_name}" --dir "${dl_dir}" >&2
    PLUGIN_ZIP="${dl_dir}/${asset_name}"
  else
    # Fallback: direct download by release URL convention.
    local url="https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/${ref}/clickhouse-kafka-connect-${ref}.zip"
    PLUGIN_ZIP="${dl_dir}/clickhouse-kafka-connect-${ref}.zip"
    echo ">> gh not available; curl ${url}" >&2
    curl -fsSL -o "${PLUGIN_ZIP}" "${url}"
    if [[ -z "${GIT_SHA}" ]]; then GIT_SHA="unknown"; fi
  fi

  if [[ ! -f "${PLUGIN_ZIP}" ]]; then
    echo "error: pinned plugin download failed" >&2; exit 1
  fi

  PLUGIN_SHA256="$(sha256_of "${PLUGIN_ZIP}")"
  if [[ -n "${expected_sha}" && "${expected_sha}" != "${PLUGIN_SHA256}" ]]; then
    echo "error: pinned artifact sha256 mismatch: release=${expected_sha} downloaded=${PLUGIN_SHA256}" >&2
    exit 1
  fi
  if [[ -n "${expected_sha}" ]]; then
    echo ">> pinned artifact sha256 verified against release digest: ${PLUGIN_SHA256}" >&2
  else
    echo ">> WARNING: release published no digest; recording downloaded sha256: ${PLUGIN_SHA256}" >&2
  fi
}

if [[ "${ARM}" == "head" ]]; then
  build_head_plugin
else
  fetch_pinned_plugin
fi

BUILD_TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SHORT_SHA="${GIT_SHA:0:12}"

if [[ -z "${IMAGE_TAG}" ]]; then
  if [[ "${ARM}" == "head" ]]; then
    IMAGE_TAG="clickhouse-kafka-connect-benchmark:head-${SHORT_SHA}"
  else
    IMAGE_TAG="clickhouse-kafka-connect-benchmark:pinned-${CONNECTOR_VERSION}"
  fi
fi

# Confluent Avro converter (review F2): download the pinned Hub archive and
# verify its sha256 against the pin — a mismatch fails the build (same
# discipline as the pinned plugin). Downloaded fresh per invocation (~9 MB).
fetch_avro_converter() {
  AVRO_CONVERTER_STAGED="avro-converter-${AVRO_CONVERTER_VERSION}.zip"
  echo ">> fetching Confluent Avro converter ${AVRO_CONVERTER_VERSION} (plan §6 insert path)" >&2
  curl -fsSL -o "${SCRIPT_DIR}/${AVRO_CONVERTER_STAGED}" "${AVRO_CONVERTER_URL}"
  local got
  got="$(sha256_of "${SCRIPT_DIR}/${AVRO_CONVERTER_STAGED}")"
  if [[ "${got}" != "${AVRO_CONVERTER_SHA256}" ]]; then
    echo "error: Avro converter sha256 mismatch: pinned=${AVRO_CONVERTER_SHA256} downloaded=${got}" >&2
    echo "       The fetched archive is not the pinned artifact. Bump the pin deliberately" >&2
    echo "       (AVRO_CONVERTER_VERSION + AVRO_CONVERTER_SHA256 together) or investigate." >&2
    rm -f "${SCRIPT_DIR}/${AVRO_CONVERTER_STAGED}"
    exit 1
  fi
  echo ">> Avro converter sha256 verified: ${got}" >&2
}
fetch_avro_converter

# IMAGE_DIGEST_REF is the immutable digest reference (repo@sha256:...) resolved
# AFTER a successful push. Digest-pinning is the benchmark default (mutable tags
# get served stale by node/registry caches). Empty until resolved.
IMAGE_DIGEST_REF=""

# Resolve the pushed image's registry digest and build the immutable
# repo@sha256:... reference. Works for BOTH ECR and ghcr: once an image is
# pushed, `docker inspect .RepoDigests` carries the registry-computed digest for
# the repo it was pushed to. We match the digest to THIS tag's repo (strip the
# ":tag" suffix) so a multi-repo local daemon can't hand back the wrong entry.
# On failure this returns non-zero and leaves IMAGE_DIGEST_REF empty; the caller
# decides whether that is fatal (it is, for the push path — a benchmark must not
# fall back to the mutable tag).
resolve_image_digest() {
  local tag="$1"
  local repo="${tag%%:*}"     # drop the ":tag"; leaves registry/host/path repo
  local digests line
  digests="$(docker inspect --format='{{range .RepoDigests}}{{println .}}{{end}}' "${tag}" 2>/dev/null)"
  while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    if [[ "${line}" == "${repo}@sha256:"* ]]; then
      IMAGE_DIGEST_REF="${line}"
      return 0
    fi
  done <<< "${digests}"
  return 1
}

# docker build needs the zips inside the build context. Stage copies next to
# the Dockerfile and reference them relatively.
STAGED_ZIP_NAME="plugin-${ARM}.zip"
cp "${PLUGIN_ZIP}" "${SCRIPT_DIR}/${STAGED_ZIP_NAME}"
cleanup_staged() {
  rm -f "${SCRIPT_DIR}/${STAGED_ZIP_NAME}" "${SCRIPT_DIR}/${AVRO_CONVERTER_STAGED}"
}

emit() {
  echo "image_ref=${IMAGE_TAG}"
  echo "arm=${ARM}"
  echo "git_sha=${GIT_SHA}"
  echo "connector_version=${CONNECTOR_VERSION}"
  echo "strimzi_image=${STRIMZI_IMAGE}"
  echo "strimzi_version=${STRIMZI_VERSION}"
  echo "kafka_version=${KAFKA_VERSION}"
  echo "plugin_zip=${PLUGIN_ZIP}"
  echo "plugin_sha256=${PLUGIN_SHA256}"
  echo "avro_converter_version=${AVRO_CONVERTER_VERSION}"
  echo "avro_converter_sha256=${AVRO_CONVERTER_SHA256}"
  # Digest line: the machine-readable, immutable reference callers must pin on.
  # Present only after a --push that resolved a registry digest.
  echo "image_digest_ref=${IMAGE_DIGEST_REF}"
  if [[ -n "${IMAGE_DIGEST_REF}" ]]; then
    echo "IMAGE_DIGEST=${IMAGE_DIGEST_REF}"
  fi
}

if [[ "${RUN_DOCKER}" -eq 0 ]]; then
  echo ">> --no-docker: skipping docker build" >&2
  if [[ "${DO_PUSH}" -eq 1 ]]; then
    echo "error: --push requires a docker build (cannot push under --no-docker)" >&2
    cleanup_staged
    exit 2
  fi
  cleanup_staged
  emit
  exit 0
fi

echo ">> docker build ${IMAGE_TAG}" >&2
docker build \
  --file "${SCRIPT_DIR}/Dockerfile" \
  --build-arg "STRIMZI_IMAGE=${STRIMZI_IMAGE}" \
  --build-arg "PLUGIN_ZIP=${STAGED_ZIP_NAME}" \
  --build-arg "AVRO_CONVERTER_ZIP=${AVRO_CONVERTER_STAGED}" \
  --build-arg "AVRO_CONVERTER_VERSION=${AVRO_CONVERTER_VERSION}" \
  --build-arg "AVRO_CONVERTER_SHA256=${AVRO_CONVERTER_SHA256}" \
  --build-arg "ARM=${ARM}" \
  --build-arg "GIT_SHA=${GIT_SHA}" \
  --build-arg "CONNECTOR_VERSION=${CONNECTOR_VERSION}" \
  --build-arg "PLUGIN_SHA256=${PLUGIN_SHA256}" \
  --build-arg "KAFKA_VERSION=${KAFKA_VERSION}" \
  --build-arg "STRIMZI_VERSION=${STRIMZI_VERSION}" \
  --build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}" \
  --tag "${IMAGE_TAG}" \
  "${SCRIPT_DIR}"

# Push + resolve digest (digest-pinning is the default; see header). A benchmark
# arm must be deployed by digest so node/registry caches cannot serve a stale
# image behind a reused tag. If the push succeeds but the digest cannot be
# resolved, FAIL LOUDLY rather than let the caller fall back to the mutable tag.
if [[ "${DO_PUSH}" -eq 1 ]]; then
  echo ">> docker push ${IMAGE_TAG}" >&2
  docker push "${IMAGE_TAG}" >&2
  if ! resolve_image_digest "${IMAGE_TAG}"; then
    echo "error: pushed ${IMAGE_TAG} but could not resolve its registry digest" >&2
    echo "       (docker inspect .RepoDigests had no ${IMAGE_TAG%%:*}@sha256: entry)." >&2
    echo "       Refusing to emit a tag-only ref — the stale-tag failure class is" >&2
    echo "       exactly what digest pinning prevents." >&2
    cleanup_staged
    exit 1
  fi
  echo ">> resolved image digest: ${IMAGE_DIGEST_REF}" >&2
  if [[ -n "${DIGEST_OUT}" ]]; then
    printf '%s\n' "${IMAGE_DIGEST_REF}" > "${DIGEST_OUT}"
  fi
fi

cleanup_staged
emit
