import csv
import sys
import json
from pathlib import Path
import statistics

ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent / ".runs" / "extended"
data = {}
for variant in ("baseline", "inline"):
    data[variant] = {}
    for number in range(1, 4):
        for result in json.loads((ROOT / f"{variant}-{number}.json").read_text())["results"]:
            data[variant].setdefault(result["case"], []).append(result)

def median(items, *keys):
    values = []
    for item in items:
        for key in keys:
            item = item[key]
        values.append(item)
    return statistics.median(values)

summary = []
for case, baseline in data["baseline"].items():
    inline = data["inline"][case]
    before = median(baseline, "queries_per_second")
    after = median(inline, "queries_per_second")
    row = {
        "case": case, "clients": baseline[0]["clients"], "workers": baseline[0]["workers"],
        "baseline_qps": round(before, 2), "inline_qps": round(after, 2),
        "throughput_gain_percent": round(100 * (after / before - 1), 2),
        "baseline_query_p99_ms": round(median(baseline, "query", "p99_ns") / 1e6, 3),
        "inline_query_p99_ms": round(median(inline, "query", "p99_ns") / 1e6, 3),
        "baseline_probe_p99_us": round(median(baseline, "probe_queue_delay", "p99_ns") / 1000, 2),
        "inline_probe_p99_us": round(median(inline, "probe_queue_delay", "p99_ns") / 1000, 2),
        "baseline_jain": round(median(baseline, "jain_fairness"), 5),
        "inline_jain": round(median(inline, "jain_fairness"), 5),
        "baseline_jain_min": round(min(x["jain_fairness"] for x in baseline), 5),
        "inline_jain_min": round(min(x["jain_fairness"] for x in inline), 5),
        "baseline_min_client_queries": min(min(x["per_client_completed"]) for x in baseline),
        "inline_min_client_queries": min(min(x["per_client_completed"]) for x in inline),
        "baseline_gets_per_query": round(median(baseline, "gets_per_query"), 3),
        "inline_gets_per_query": round(median(inline, "gets_per_query"), 3),
        "baseline_qps_min": min(x["queries_per_second"] for x in baseline),
        "baseline_qps_max": max(x["queries_per_second"] for x in baseline),
        "inline_qps_min": min(x["queries_per_second"] for x in inline),
        "inline_qps_max": max(x["queries_per_second"] for x in inline),
        "get_errors": sum(x["get_errors"] for x in baseline + inline),
    }
    summary.append(row)
(ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
with (ROOT / "summary.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(summary[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(summary)
for row in summary:
    print(f"{row['case']:40} qps {row['baseline_qps']:7.1f} -> {row['inline_qps']:7.1f} "
          f"({row['throughput_gain_percent']:+6.1f}%), "
          f"p99 {row['baseline_query_p99_ms']:7.2f} -> {row['inline_query_p99_ms']:7.2f} ms, "
          f"probe {row['baseline_probe_p99_us']:7.1f} -> {row['inline_probe_p99_us']:7.1f} us, "
          f"Jain {row['baseline_jain']:.4f} -> {row['inline_jain']:.4f}")
