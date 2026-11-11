"""Turn JUnit XML (build/test-results) and build/reports/lag.json into a Markdown evidence summary."""
import glob, json, os, xml.etree.ElementTree as ET

out = ["## Event-streaming evidence", ""]
rows, tests, failures, errors, time = [], 0, 0, 0, 0.0
for f in sorted(glob.glob("build/test-results/*/TEST-*.xml")):
    suite = ET.parse(f).getroot()
    tests += int(suite.get("tests")); failures += int(suite.get("failures")); errors += int(suite.get("errors"))
    time += float(suite.get("time"))
    for case in suite.iter("testcase"):
        failed = case.find("failure") is not None or case.find("error") is not None
        rows.append(f"| {'❌' if failed else '✅'} | `{case.get('classname').replace('lab.', '', 1)}` {case.get('name')} | {float(case.get('time')):.2f} |")
out.append(f"**{tests} tests, {failures} failures, {errors} errors, {time:.1f}s** (main suite in parallel, then the broker-outage and lag tests alone)")
out += ["", "| Result | Test | Time (s) |", "|---|---|---|", *rows]
if os.path.exists("build/reports/lag.json"):
    l = json.load(open("build/reports/lag.json"))
    out += ["", "### Consumer lag burst", "",
            f"{l['events']} events, drained in **{l['drainMs']} ms** (budget {l['budgetMs']} ms), ~{l['eventsPerSec']} events/s"]
print("\n".join(out))
