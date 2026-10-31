import csv
import sys
import json
from pathlib import Path
import statistics

ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent / ".runs" / "single"
summary = []
for caller in ("external", "worker"):
    measurements = {}
    for variant in ("baseline", "inline"):
        runs = [json.loads((ROOT / f"{caller}-{variant}-{number}.json").read_text())
                for number in range(1, 4)]
        measurements[variant] = {
            item["case"]: [next(value for value in run["results"] if value["case"] == item["case"])
                            for run in runs]
            for item in runs[0]["results"]
        }
    for case, before in measurements["baseline"].items():
        after = measurements["inline"][case]
        before_p50 = statistics.median(item["p50_ns"] for item in before)
        after_p50 = statistics.median(item["p50_ns"] for item in after)
        row = {
            "caller": caller,
            "case": case,
            "baseline_p50_us": round(before_p50 / 1000, 3),
            "inline_p50_us": round(after_p50 / 1000, 3),
            "latency_reduction_percent": round((1 - after_p50 / before_p50) * 100, 2),
            "speedup": round(before_p50 / after_p50, 3),
            "baseline_p95_us": round(statistics.median(item["p95_ns"] for item in before) / 1000, 3),
            "inline_p95_us": round(statistics.median(item["p95_ns"] for item in after) / 1000, 3),
            "baseline_p50_min_us": round(min(item["p50_ns"] for item in before) / 1000, 3),
            "baseline_p50_max_us": round(max(item["p50_ns"] for item in before) / 1000, 3),
            "inline_p50_min_us": round(min(item["p50_ns"] for item in after) / 1000, 3),
            "inline_p50_max_us": round(max(item["p50_ns"] for item in after) / 1000, 3),
            "rounds": 3,
            "samples_per_round": before[0]["samples"],
            "rows": before[0]["rows"],
        }
        summary.append(row)
(ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
with (ROOT / "summary.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(summary[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(summary)
for row in summary:
    print(f"{row['caller']:8} {row['case']:32} "
          f"{row['baseline_p50_us']:10.3f} -> {row['inline_p50_us']:10.3f} us "
          f"({row['latency_reduction_percent']:+6.2f}%)")
