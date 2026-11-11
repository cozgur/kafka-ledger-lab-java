#!/usr/bin/env bash
# Run the whole suite N times and report per-test failures.
# --rerun is essential and applies only to the task right before it: Gradle otherwise marks an unchanged test task
# UP-TO-DATE and runs nothing (an earlier version passed `test perfTest --rerun` and re-ran only perfTest).
# Usage: scripts/flaky-check.sh [N=20]
set -uo pipefail
N="${1:-20}"
OUT="build/flaky"
rm -rf "$OUT" && mkdir -p "$OUT"
pass=0; fail=0; t_all=$(date +%s)
for i in $(seq 1 "$N"); do
  t0=$(date +%s)
  if ./gradlew test --rerun perfTest --rerun --console=plain >"$OUT/run-$i.log" 2>&1 \
    && ! grep -qE "Task :(test|perfTest) UP-TO-DATE" "$OUT/run-$i.log"; then  # a skipped task is not a pass
    pass=$((pass + 1)); r=PASS
  else
    fail=$((fail + 1)); r=FAIL
  fi
  mkdir -p "$OUT/run-$i" && cp build/test-results/*/TEST-*.xml "$OUT/run-$i/" 2>/dev/null
  echo "run $i/$N: $r ($(( $(date +%s) - t0 ))s)"
done
echo "suite runs: $N, passed: $pass, failed: $fail, total: $(( $(date +%s) - t_all ))s"
python3 scripts/flaky_summary.py "$OUT"
[ "$fail" -eq 0 ]
