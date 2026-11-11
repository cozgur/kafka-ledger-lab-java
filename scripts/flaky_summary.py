"""Summarise build/flaky/run-*/TEST-*.xml into build/flaky/summary.md."""
import glob, sys, xml.etree.ElementTree as ET
from collections import defaultdict

d = sys.argv[1] if len(sys.argv) > 1 else "build/flaky"
runs = sorted(glob.glob(f"{d}/run-*/"))
stats = defaultdict(lambda: [0, 0, 0.0])  # runs, failures, max seconds
for r in runs:
    for f in glob.glob(f"{r}TEST-*.xml"):
        for c in ET.parse(f).getroot().iter("testcase"):
            s = stats[f"{c.get('classname').replace('lab.', '', 1)} > {c.get('name')}"]
            s[0] += 1
            s[1] += int(c.find("failure") is not None or c.find("error") is not None)
            s[2] = max(s[2], float(c.get("time")))
total_fail = sum(s[1] for s in stats.values())
executions = sum(s[0] for s in stats.values())
lines = [f"# Flakiness check: {len(runs)} runs, {len(stats)} distinct tests, {executions} executions, {total_fail} failures", "",
         "Runs counts only tests whose XML was produced in that run (results are cleared before each run).", "",
         "| Test | Runs | Failures | Max time (s) |", "|---|---|---|---|"]
lines += [f"| {n} | {s[0]} | {s[1]} | {s[2]:.2f} |" for n, s in sorted(stats.items())]
open(f"{d}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines[:1]))
