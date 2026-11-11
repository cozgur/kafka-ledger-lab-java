#!/usr/bin/env bash
# Run the whole suite N times and report per-test failures, counting only tests that really ran in that run.
# - --rerun applies only to the task right before it, so each task gets its own (else Gradle reports UP-TO-DATE and runs nothing)
# - results are deleted before every run and --continue runs the later tasks even if an earlier one fails, so a run can never
#   be credited with XML files left over from a previous run (both happened in earlier versions of this script)
# Usage: scripts/flaky-check.sh [N=20]
set -uo pipefail
N="${1:-20}"
OUT="build/flaky"
TASKS=(test isolatedTest perfTest)
rm -rf "$OUT" && mkdir -p "$OUT"
pass=0; fail=0; t_all=$(date +%s)
for i in $(seq 1 "$N"); do
  t0=$(date +%s)
  rm -rf build/test-results build/test-logs
  ./gradlew --continue test --rerun isolatedTest --rerun perfTest --rerun --console=plain >"$OUT/run-$i.log" 2>&1
  rc=$?
  ran=true
  for t in "${TASKS[@]}"; do
    grep -q "^> Task :$t\$" "$OUT/run-$i.log" && ! grep -qE "Task :$t (UP-TO-DATE|SKIPPED)" "$OUT/run-$i.log" || ran=false
    [ -d "build/test-results/$t" ] || ran=false
  done
  if [ $rc -eq 0 ] && $ran; then pass=$((pass + 1)); r=PASS; else fail=$((fail + 1)); r="FAIL(rc=$rc,all-tasks-ran=$ran)"; fi
  mkdir -p "$OUT/run-$i" && cp build/test-results/*/TEST-*.xml "$OUT/run-$i/" 2>/dev/null
  [ -d build/test-logs ] && cp -r build/test-logs "$OUT/run-$i/"
  echo "run $i/$N: $r ($(( $(date +%s) - t0 ))s)"
done
echo "suite runs: $N, passed: $pass, failed: $fail, total: $(( $(date +%s) - t_all ))s"
python3 scripts/flaky_summary.py "$OUT"
[ "$fail" -eq 0 ]
