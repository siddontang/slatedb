# Issue 2166: inline scan fetches

This experiment reduces scan latency but increases the queue delay for other tasks.
The patch needs more scheduling work before an upstream merge.

The experiment starts from upstream commit `65514581173f05b586a0ffc6d8351278bfddba2a`.
It polls fetches inline when `max_fetch_tasks == 1` and removes the point and single-block guards.
The baseline restores both guards while it retains the same benchmark code.

## Setup

The host uses an Apple M4, 10 CPU cores, 24 GiB RAM, and macOS 27.0.1.
The tests use Rust 1.91.1 and the release profile.
Each ordinary scan reads 2,048 keys with 128-byte values.
The large scan cases read 32,768 keys.
The default fetch size is one data block.

Foyer uses a 64 MiB cache with four shards.
The database disables compaction and garbage collection.
The tests flush each batch to an L0 SST before the timed phase.
The eight-SST cases place versions of the same keys in eight overlapping L0 SSTs.

## Single client

These values are medians of three run-level p50 measurements.
Latency reduction is positive when the patch runs faster.
The complete table contains 36 cases in [single-summary.csv](results/single-summary.csv).

| Caller | SSTs | Scan | Baseline ms | Inline ms | Reduction |
| --- | ---: | --- | ---: | ---: | ---: |
| `block_on` | 1 | Full | 2.197 | 1.444 | 34.3% |
| `block_on` | 8 | Full | 14.352 | 9.640 | 32.8% |
| `block_on` | 1 | Descending | 2.355 | 1.595 | 32.3% |
| `block_on` | 1 | 64 KiB fetch | 1.533 | 1.433 | 6.5% |
| `block_on` | 1 | Four fetch tasks | 1.867 | 1.870 | -0.2% |
| Tokio worker | 1 | Full | 1.700 | 1.524 | 10.4% |
| Tokio worker | 8 | Full | 10.098 | 9.377 | 7.1% |

The `block_on` caller drives the scan from the calling thread.
The worker caller drives the scan inside a spawned Tokio task.
These are native Rust calls without binding overhead.
The simulated 1 ms storage-delay control shows less than 1% improvement.

An initial worker result suggested a regression for eight SSTs with 64 KiB fetches.
The focused rerun used 600 samples per run and three runs per variant.
It measured 9.267 ms before the patch and 9.213 ms after the patch.
That difference is less than 1%.
See [single-recheck-summary.json](results/single-recheck-summary.json).

## Concurrent clients

The initial matrix covers 1, 8, and 32 clients with one or two Tokio workers.
The focused tests use eight clients, two workers, and five interleaved runs per variant.
Each run measures a shared two-second window.
Clients issue requests in a closed loop.
Each client yields between queries unless the case name contains `noyield`.

A separate OS thread submits a lightweight Tokio task every millisecond.
The probe records the time from submission until the task starts.
Its queue delay measures scheduler response, not database query latency.
The table shows medians of five run-level measurements.

| Case | Baseline scans/s | Inline scans/s | Baseline probe p99 ms | Inline probe p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Eight clients | 576.5 | 577.5 | 0.163 | 0.398 |
| No extra yield between queries | 576.0 | 618.5 | 0.164 | 0.369 |
| Four fetch tasks, control | 619.5 | 611.0 | 0.207 | 0.202 |

The eight-client probe ranges do not overlap across the five runs.
Baseline p99 ranges from 0.150 to 0.165 ms.
Inline p99 ranges from 0.306 to 0.412 ms.
This supports a repeatable scheduler-delay regression for the measured workload.

The Jain index measures the distribution of completed queries across clients.
A value of one means equal completion counts.
The eight-client index changes from 0.9987 to 0.9998.
Every client completes queries in every recorded phase.
The probe still waits longer despite this even distribution.

See [focus-summary.csv](results/focus-summary.csv) for ranges and query p99 values.
The earlier, noisier matrix remains in [matrix-summary.csv](results/matrix-summary.csv).

## RustFS S3

These tests use the installed RustFS 1.0.4 binary through loopback HTTP.
Uncached cases disable SlateDB data-block caching and retain cached metadata.
The warm control reads data blocks from Foyer.
The tests do not clear RustFS or operating-system caches.

| Case | Clients | Baseline scans/s | Inline scans/s | Change |
| --- | ---: | ---: | ---: | ---: |
| One-block fetch | 1 | 56.5 | 62.0 | +9.7% |
| One-block fetch | 8 | 87.5 | 96.0 | +9.7% |
| 64 KiB fetch | 8 | 320.0 | 276.0 | -13.8% |
| Four fetch tasks, control | 8 | 79.0 | 80.0 | +1.3% |
| Warm-cache control | 8 | 312.0 | 284.0 | -9.0% |

The one-block cases issue about 76 GETs per scan.
The 64 KiB case issues about five GETs per scan.
The 158 concurrency and S3 phases issue 820,058 S3 GET requests in total.
Each scan checks key order, value bytes, and row count.
The tests record no unexpected GET errors.
They count normal `NotFound` background probes separately.

Host CPU load causes large S3 sample variations.
For example, the eight-client baseline ranges from 66 to 176 scans/s.
The inline version ranges from 57 to 184.5 scans/s.
The 64 KiB throughput decrease remains an observation without a confirmed cause.
These measurements do not establish a general S3 improvement or regression.

## Inspect recorded results

The [raw archive](results/raw-samples.tar.gz) contains 110 JSON files with individual samples.
The [archive manifest](results/archive-manifest.json) lists file hashes.
The [metadata](results/metadata.json) records the original environment and binary hashes.
The archive excludes binaries, server logs, and temporary credentials.

Run these commands from the repository root:

```sh
python3 experiments/issue-2166/reproduce.py unpack
python3 experiments/issue-2166/summarize_single.py
python3 experiments/issue-2166/summarize_matrix.py
python3 experiments/issue-2166/summarize_focus.py
```

## Reproduce the comparison

Build from a clean commit on this experiment branch.
The helper creates temporary worktrees and leaves the current source intact.
Both variants use the same harness and build profile.
Only the baseline restores the original inline eligibility checks.

```sh
python3 experiments/issue-2166/reproduce.py build
python3 experiments/issue-2166/reproduce.py run --suite single
python3 experiments/issue-2166/reproduce.py run --suite recheck
```

For S3 cases, install RustFS and the AWS CLI first.
The helper reuses `rustfs` from `PATH`.
It creates a loopback server with separate data, a free port, and random credentials.
Stop the test server after the runs to remove its data and credentials.

```sh
python3 experiments/issue-2166/rustfs_session.py start
python3 experiments/issue-2166/reproduce.py run --suite matrix
python3 experiments/issue-2166/reproduce.py run --suite focus
python3 experiments/issue-2166/rustfs_session.py stop
```

To run the memory cases alone:

```sh
python3 experiments/issue-2166/reproduce.py run --suite focus --filter memory/
```

The helpers write local output under the ignored `.runs` directory.
They leave the recorded files under `results` intact.

## Validation and next step

The patch passes 84 SST iterator tests and 229 related reader and iterator tests.
These include 42 new cases for cancellation, drop, error propagation, and scan ordering.
Clippy and formatting checks pass with default features.
The next experiment needs to address scheduler response between scan batches.

AI assistance: Codex wrote the experimental patch, tests, benchmark harness, scripts, and report.
