#!/usr/bin/env bash
#
# check_contract_sync.sh — Benchmark v2 contract & perf.* DDL sync guard (task 38).
#
# WHY THIS EXISTS (benchmark-v2-contract.md §5):
#   spark-clickhouse-connector OWNS the Benchmark v2 contract doc and the perf.*
#   DDL. clickhouse-kafka-connect carries a BYTE-IDENTICAL vendored copy. §5 permits
#   a vendored copy ONLY if a CI sync check diffs it against the canonical files and
#   fails the build on ANY difference.
#
#   Every perf.* DDL file uses `CREATE ... IF NOT EXISTS`. A drifted vendored copy
#   (an added column, a changed type) therefore SILENTLY NO-OPS against the already
#   created shared tables and hides the drift indefinitely — the two repos would
#   believe they share a schema they do not. `IF NOT EXISTS` cannot detect drift.
#   THIS CHECK IS THE ONLY ALARM. If it does not run, drift ships unnoticed.
#
# TWO MODES:
#   1. Pinned-hash mode (default, CI-feasible):
#        Verifies the sha256 of the 5 vendored files against the pinned manifest
#        contract-sync.sha256 (shasum -c compatible). This is a PROXY for the true
#        §5 check: it pins the exact bytes that were reviewed and vendored, but it
#        cannot notice if BOTH the manifest and the file were changed together, nor
#        if the canonical upstream file moved. It is the CI-feasible guard until the
#        Spark branch merges upstream and the canonical files are fetchable in CI.
#
#   2. Upstream byte-diff mode (the TRUE §5 check, opt-in):
#        If env UPSTREAM_SPARK_REPO points at a local checkout of
#        spark-clickhouse-connector, ALSO byte-diff (cmp) each vendored file against
#        its canonical source there and fail on any difference. This is the real
#        contract: vendored == canonical, byte-for-byte.
#
#   Run mode 2 locally before vendoring a contract/DDL update; CI runs mode 1.
#
# USAGE:
#   benchmarks/e2e/scripts/check_contract_sync.sh
#   UPSTREAM_SPARK_REPO=/path/to/spark-clickhouse-connector benchmarks/e2e/scripts/check_contract_sync.sh
#
# UPDATE PROCEDURE (when the contract or perf.* DDL legitimately changes):
#   1. The change MUST land in spark-clickhouse-connector FIRST (contract preamble
#      change-control rule); the Kafka side MUST ack it.
#   2. Re-vendor the changed file(s) BYTE-IDENTICAL from the canonical Spark files.
#   3. Regenerate the manifest:
#        cd <repo root>
#        ( cd . && shasum -a 256 \
#            docs/benchmark-v2-contract.md \
#            benchmarks/e2e/sql/perf/01_create_database.sql \
#            benchmarks/e2e/sql/perf/02_create_runs.sql \
#            benchmarks/e2e/sql/perf/03_create_metrics.sql \
#            benchmarks/e2e/sql/perf/04_create_ch_inserts.sql \
#        ) > benchmarks/e2e/scripts/contract-sync.sha256
#   4. Run this script with UPSTREAM_SPARK_REPO set to confirm byte-identity.
#
set -euo pipefail

# Resolve repo root from this script's location (scripts live at
# <root>/benchmarks/e2e/scripts/), independent of the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MANIFEST="$SCRIPT_DIR/contract-sync.sha256"

# The vendored files, relative to REPO_ROOT. Keep in sync with the manifest and
# with the contract §5 file list.
VENDORED_FILES=(
  "docs/benchmark-v2-contract.md"
  "benchmarks/e2e/sql/perf/01_create_database.sql"
  "benchmarks/e2e/sql/perf/02_create_runs.sql"
  "benchmarks/e2e/sql/perf/03_create_metrics.sql"
  "benchmarks/e2e/sql/perf/04_create_ch_inserts.sql"
)

# Map each vendored path to its canonical path inside a spark-clickhouse-connector
# checkout (for UPSTREAM_SPARK_REPO byte-diff mode).
canonical_path_for() {
  case "$1" in
    "docs/benchmark-v2-contract.md")                    echo "docs/benchmark-v2-contract.md" ;;
    "benchmarks/e2e/sql/perf/01_create_database.sql")   echo "benchmarks/sql/perf/01_create_database.sql" ;;
    "benchmarks/e2e/sql/perf/02_create_runs.sql")       echo "benchmarks/sql/perf/02_create_runs.sql" ;;
    "benchmarks/e2e/sql/perf/03_create_metrics.sql")    echo "benchmarks/sql/perf/03_create_metrics.sql" ;;
    "benchmarks/e2e/sql/perf/04_create_ch_inserts.sql") echo "benchmarks/sql/perf/04_create_ch_inserts.sql" ;;
    *) return 1 ;;
  esac
}

fail() {
  echo "" >&2
  echo "  CONTRACT SYNC CHECK FAILED" >&2
  echo "" >&2
  echo "  $*" >&2
  echo "" >&2
  echo "  WHY THIS MATTERS (benchmark-v2-contract.md §5):" >&2
  echo "    The perf.* DDL uses CREATE ... IF NOT EXISTS, so a drifted vendored copy" >&2
  echo "    SILENTLY NO-OPS against the already-created shared tables and hides the" >&2
  echo "    drift indefinitely. IF NOT EXISTS cannot detect drift — this check is the" >&2
  echo "    ONLY alarm. spark-clickhouse-connector OWNS this DDL and the contract doc;" >&2
  echo "    the Kafka repo MUST carry a byte-identical copy." >&2
  echo "" >&2
  echo "    Do NOT paper over this by regenerating the manifest to match a local edit." >&2
  echo "    Structural changes MUST land in spark-clickhouse-connector FIRST, be acked" >&2
  echo "    by the Kafka side, then be re-vendored byte-identical. See the UPDATE" >&2
  echo "    PROCEDURE in the header of this script." >&2
  echo "" >&2
  exit 1
}

cd "$REPO_ROOT"

echo "== Benchmark v2 contract sync check =="
echo "repo root:  $REPO_ROOT"
echo "manifest:   $MANIFEST"

[ -f "$MANIFEST" ] || fail "pinned manifest not found: $MANIFEST"

# Every vendored file must exist before we hash it.
missing=0
for f in "${VENDORED_FILES[@]}"; do
  if [ ! -f "$f" ]; then
    echo "  MISSING vendored file: $f" >&2
    missing=1
  fi
done
[ "$missing" -eq 0 ] || fail "one or more vendored files are missing (listed above)."

# ---- Mode 1: pinned-hash verification (shasum -c against the manifest) ----
echo ""
echo "-- Mode 1: pinned-hash verification --"
if command -v shasum >/dev/null 2>&1; then
  SHASUM_C=(shasum -a 256 -c "$MANIFEST")
elif command -v sha256sum >/dev/null 2>&1; then
  SHASUM_C=(sha256sum -c "$MANIFEST")
else
  fail "neither shasum nor sha256sum is available on PATH."
fi

if ! "${SHASUM_C[@]}"; then
  fail "sha256 of a vendored file does NOT match the pinned manifest (see FAILED line above from shasum -c). The vendored file drifted from the reviewed/pinned bytes."
fi
echo "  all 5 vendored files match the pinned manifest."

# ---- Mode 2: upstream byte-diff (the true §5 check, opt-in) ----
if [ -n "${UPSTREAM_SPARK_REPO:-}" ]; then
  echo ""
  echo "-- Mode 2: upstream byte-diff against UPSTREAM_SPARK_REPO --"
  echo "  UPSTREAM_SPARK_REPO=$UPSTREAM_SPARK_REPO"
  [ -d "$UPSTREAM_SPARK_REPO" ] || fail "UPSTREAM_SPARK_REPO is set but is not a directory: $UPSTREAM_SPARK_REPO"
  for f in "${VENDORED_FILES[@]}"; do
    canon_rel="$(canonical_path_for "$f")" || fail "no canonical mapping for vendored file: $f"
    canon="$UPSTREAM_SPARK_REPO/$canon_rel"
    [ -f "$canon" ] || fail "canonical file not found in UPSTREAM_SPARK_REPO: $canon"
    if ! cmp -s "$f" "$canon"; then
      fail "vendored file DIFFERS byte-for-byte from canonical: $f  !=  $canon (this is the true §5 violation)."
    fi
    echo "  OK (byte-identical): $f == $canon_rel"
  done
  echo "  all 5 vendored files are byte-identical to the canonical Spark files."
else
  echo ""
  echo "-- Mode 2 skipped: UPSTREAM_SPARK_REPO not set --"
  echo "  (set it to a local spark-clickhouse-connector checkout to run the true byte-diff §5 check)"
fi

echo ""
echo "CONTRACT SYNC CHECK PASSED"
