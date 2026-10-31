import csv
import sys
import json
from pathlib import Path
import statistics

ROOT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent / ".runs" / "extended"
groups = {}
focus_files = list(ROOT.glob("focus-*-baseline-[1-5].json")) + list(ROOT.glob("focus-*-inline-[1-5].json"))
for file in focus_files:
    item = json.loads(file.read_text())["results"][0]
    variant = "baseline" if "-baseline-" in file.name else "inline"
    groups.setdefault(item["case"], {}).setdefault(variant, []).append(item)

def median(items, *keys):
    values = []
    for item in items:
        for key in keys:
            item = item[key]
        values.append(item)
    return statistics.median(values)

summary = []
for case, variants in groups.items():
    before, after = variants["baseline"], variants["inline"]
    assert len(before) == len(after) == 5
    bq, aq = median(before, "queries_per_second"), median(after, "queries_per_second")
    bp, ap = median(before, "probe_queue_delay", "p99_ns"), median(after, "probe_queue_delay", "p99_ns")
    row = {
        "case": case, "rounds_per_variant": 5, "phase_duration_ms": 2000,
        "baseline_qps": bq, "inline_qps": aq, "throughput_gain_percent": round(100*(aq/bq-1),2),
        "baseline_qps_min": min(x["queries_per_second"] for x in before),
        "baseline_qps_max": max(x["queries_per_second"] for x in before),
        "inline_qps_min": min(x["queries_per_second"] for x in after),
        "inline_qps_max": max(x["queries_per_second"] for x in after),
        "baseline_query_p99_ms": round(median(before,"query","p99_ns")/1e6,3),
        "inline_query_p99_ms": round(median(after,"query","p99_ns")/1e6,3),
        "baseline_probe_p99_us": round(bp/1000,3), "inline_probe_p99_us": round(ap/1000,3),
        "probe_p99_increase_percent": round(100*(ap/bp-1),2),
        "baseline_probe_p99_min_us": min(x["probe_queue_delay"]["p99_ns"] for x in before)/1000,
        "baseline_probe_p99_max_us": max(x["probe_queue_delay"]["p99_ns"] for x in before)/1000,
        "inline_probe_p99_min_us": min(x["probe_queue_delay"]["p99_ns"] for x in after)/1000,
        "inline_probe_p99_max_us": max(x["probe_queue_delay"]["p99_ns"] for x in after)/1000,
        "baseline_jain": round(median(before,"jain_fairness"),6),
        "inline_jain": round(median(after,"jain_fairness"),6),
        "minimum_client_completed": min(min(x["per_client_completed"]) for x in before+after),
        "baseline_gets_per_query": round(median(before,"gets_per_query"),3),
        "inline_gets_per_query": round(median(after,"gets_per_query"),3),
        "unexpected_get_errors": sum(x["get_errors"] for x in before+after),
    }
    summary.append(row)
summary.sort(key=lambda r:r["case"])
(ROOT/"focus-summary.json").write_text(json.dumps(summary,indent=2)+"\n")
with (ROOT/"focus-summary.csv").open("w",newline="") as output:
    writer=csv.DictWriter(output,fieldnames=list(summary[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(summary)
for row in summary:
    print(f"{row['case']:40} qps {row['baseline_qps']:7.1f} -> {row['inline_qps']:7.1f} "
          f"({row['throughput_gain_percent']:+6.1f}%), probe "
          f"{row['baseline_probe_p99_us']:8.1f} -> {row['inline_probe_p99_us']:8.1f} us, "
          f"Jain {row['baseline_jain']:.4f} -> {row['inline_jain']:.4f}")

all_results=[]
files=list(ROOT.glob("baseline-[123].json"))+list(ROOT.glob("inline-[123].json"))+focus_files
for file in files:
    all_results.extend(json.loads(file.read_text())["results"])
print("Scenario phases:",len(all_results))
print("S3 GET requests:",sum(x["get_requests"] for x in all_results if x["case"].startswith("s3/")))
print("Unexpected GET errors:",sum(x["get_errors"] for x in all_results))
print("Minimum completed queries per client:",min(min(x["per_client_completed"]) for x in all_results))
