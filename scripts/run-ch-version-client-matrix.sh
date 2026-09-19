#!/usr/bin/env bash

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CH_VERSIONS="${CH_VERSIONS:-24.8,25.2,25.8,26.3,latest}"
CLIENT_VERSIONS="${CLIENT_VERSIONS:-V1,V2}"
TEST_TASK="${TEST_TASK:-test}"
MATRIX_GRADLE_ARGS="${MATRIX_GRADLE_ARGS:-}"
CONTINUE_ON_FAILURE="${CONTINUE_ON_FAILURE:-true}"

if [[ -n "${MATRIX_OUTPUT_DIR:-}" ]]; then
    OUTPUT_DIR="${MATRIX_OUTPUT_DIR}"
else
    TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
    OUTPUT_DIR="${ROOT_DIR}/build/matrix-test-results/${TIMESTAMP}"
fi

mkdir -p "${OUTPUT_DIR}"
SUMMARY_FILE="${OUTPUT_DIR}/summary.tsv"
echo -e "clickhouse_version\tclient_version\ttask\texit_code\tlog_file" > "${SUMMARY_FILE}"

IFS=',' read -r -a CH_LIST <<< "${CH_VERSIONS}"
IFS=',' read -r -a CLIENT_LIST <<< "${CLIENT_VERSIONS}"
read -r -a EXTRA_ARGS <<< "${MATRIX_GRADLE_ARGS}"

TOTAL=0
FAILURES=0

for ch_version in "${CH_LIST[@]}"; do
    for client_version in "${CLIENT_LIST[@]}"; do
        TOTAL=$((TOTAL + 1))
        LOG_FILE="${OUTPUT_DIR}/attempt_ch-${ch_version}_client-${client_version}.log"

        CMD=("${ROOT_DIR}/gradlew" "--no-daemon" "${TEST_TASK}")
        if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
            CMD+=("${EXTRA_ARGS[@]}")
        fi

        echo "[${TOTAL}] CLICKHOUSE_VERSION=${ch_version} CLIENT_VERSION=${client_version} TASK=${TEST_TASK}"
        if CLICKHOUSE_VERSION="${ch_version}" CLIENT_VERSION="${client_version}" "${CMD[@]}" > "${LOG_FILE}" 2>&1; then
            EXIT_CODE=0
            STATUS="PASS"
        else
            EXIT_CODE=$?
            STATUS="FAIL"
            FAILURES=$((FAILURES + 1))
        fi

        echo -e "${ch_version}\t${client_version}\t${TEST_TASK}\t${EXIT_CODE}\t${LOG_FILE}" >> "${SUMMARY_FILE}"
        echo "  -> ${STATUS}: ${LOG_FILE}"

        if [[ "${CONTINUE_ON_FAILURE}" != "true" && ${EXIT_CODE} -ne 0 ]]; then
            echo
            echo "Stopping early due to failure (CONTINUE_ON_FAILURE=${CONTINUE_ON_FAILURE})."
            exit "${EXIT_CODE}"
        fi
    done
done

echo
echo "Matrix run finished. Total attempts: ${TOTAL}, failures: ${FAILURES}"
echo "Summary file: ${SUMMARY_FILE}"

if [[ ${FAILURES} -gt 0 ]]; then
    exit 1
fi
