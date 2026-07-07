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
#   --no-docker  build the plugin zip and print the provenance/build args only;
#                skip `docker build` (used when docker is unavailable).
#
# Env overrides (aligned with task 25's Strimzi/Kafka pin; see Dockerfile default):
#   STRIMZI_IMAGE, STRIMZI_VERSION, KAFKA_VERSION.
#
# Emits, on success, KEY=VALUE lines on stdout (image_ref, git_sha, arm,
# connector_version, strimzi/kafka versions, plugin_zip) for the caller/orchestrator.

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

ARM=""
IMAGE_TAG=""
RUN_DOCKER=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arm) ARM="$2"; shift 2 ;;
    --image-tag) IMAGE_TAG="$2"; shift 2 ;;
    --no-docker) RUN_DOCKER=0; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

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

# docker build needs the zip inside the build context. Stage a copy next to the
# Dockerfile and reference it relatively.
STAGED_ZIP_NAME="plugin-${ARM}.zip"
cp "${PLUGIN_ZIP}" "${SCRIPT_DIR}/${STAGED_ZIP_NAME}"
cleanup_staged() { rm -f "${SCRIPT_DIR}/${STAGED_ZIP_NAME}"; }

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
}

if [[ "${RUN_DOCKER}" -eq 0 ]]; then
  echo ">> --no-docker: skipping docker build" >&2
  cleanup_staged
  emit
  exit 0
fi

echo ">> docker build ${IMAGE_TAG}" >&2
docker build \
  --file "${SCRIPT_DIR}/Dockerfile" \
  --build-arg "STRIMZI_IMAGE=${STRIMZI_IMAGE}" \
  --build-arg "PLUGIN_ZIP=${STAGED_ZIP_NAME}" \
  --build-arg "ARM=${ARM}" \
  --build-arg "GIT_SHA=${GIT_SHA}" \
  --build-arg "CONNECTOR_VERSION=${CONNECTOR_VERSION}" \
  --build-arg "PLUGIN_SHA256=${PLUGIN_SHA256}" \
  --build-arg "KAFKA_VERSION=${KAFKA_VERSION}" \
  --build-arg "STRIMZI_VERSION=${STRIMZI_VERSION}" \
  --build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}" \
  --tag "${IMAGE_TAG}" \
  "${SCRIPT_DIR}"

cleanup_staged
emit
