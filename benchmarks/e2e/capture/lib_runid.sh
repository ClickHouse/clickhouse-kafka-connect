#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Ported from spark-clickhouse-connector benchmarks/scripts/lib_runid.sh.
#
# Generates the canonical RUN_ID / RUN_START for a benchmark invocation and
# exports them. RUN_ID doubles as the pair's pair_id (contract §1.2):
#
#   RUN_ID = <UTC timestamp>-<short git sha>
#          = $(date -u +%Y-%m-%dT%H-%M-%SZ)-$(git rev-parse --short HEAD)
#   example: 2026-07-07T04-15-32Z-91ac2dd
#
# Per-(arm,tier) run_id is then derived by the orchestrator as
#   <RUN_ID>-<arm>-t<tier>  (contract §1.2, e.g. ...-head-t1).
#
# CONTRACT §1.2 MUST-FAIL: git rev-parse can fail (detached/no-git checkout),
# yielding a '…-nogit' identifier. Such an id does NOT pin the commit under test
# and is useless as a pair key, so this library HARD-FAILS rather than emitting
# it. Source this file (`. lib_runid.sh`) — on the nogit path it prints an error
# and `return 1`s (or exits 1 if run directly), so the caller never proceeds with
# an unpinned RUN_ID.

_short_sha="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"

if [ "${_short_sha}" = "nogit" ]; then
  echo "ERROR: git rev-parse --short HEAD failed; refusing to emit a '-nogit' RUN_ID." >&2
  echo "       Per contract §1.2 an identifier that does not pin the commit under" >&2
  echo "       test is useless as a pair key. Fix the git checkout and re-run." >&2
  # `return` works when sourced; fall through to `exit` when executed directly.
  return 1 2>/dev/null || exit 1
fi

_ts="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
export RUN_ID="${_ts}-${_short_sha}"
export RUN_START="$(date -u +%Y-%m-%dT%H:%M:%S)"
