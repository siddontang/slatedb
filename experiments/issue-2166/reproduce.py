import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
WORK = HERE / ".runs"
SINGLE_TEST = "sst_iter::bench2166::issue2166_scan_benchmark"
CONCURRENT_TEST = "sst_iter::bench2166_concurrency::issue2166_concurrency_and_s3"
FOCUS_CASES = (
    "memory/warm/8clients", "memory/warm/noyield/8clients", "memory/warm/tasks4/8clients",
    "s3/uncached/1client", "s3/uncached/8clients", "s3/uncached/batch64k/8clients",
    "s3/uncached/tasks4/8clients", "s3/warm/8clients",
)
MATRIX_CASES = (
    "memory/warm/1client", "memory/warm/8clients", "memory/warm/32clients",
    "memory/warm/large/8clients", "memory/warm/noyield/8clients",
    "memory/warm/large/oneworker/8clients", "memory/warm/tasks4/8clients",
    "s3/uncached/1client", "s3/uncached/8clients", "s3/uncached/32clients",
    "s3/uncached/batch64k/8clients", "s3/uncached/tasks4/8clients", "s3/warm/8clients",
)


def baseline_policy(source):
    replacements = (
        ("let inline_fetch = options.max_fetch_tasks == 1;",
         "let inline_fetch = view.point_key().is_some() && options.max_fetch_tasks == 1;"),
        ("let sst_level = self.sst_level().cloned();\n        let read = async move {",
         "let sst_level = self.sst_level().cloned();\n        let single_block = blocks.len() == 1;\n        let read = async move {"),
        ("if self.inline_fetch {", "if self.inline_fetch && single_block {"),
    )
    for before, after in replacements:
        if source.count(before) != 1:
            raise ValueError("The source does not match this experiment.")
        source = source.replace(before, after, 1)
    return source


def build():
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO):
        raise SystemExit("Commit the experiment before building comparison binaries.")
    target = Path(os.environ.get("CARGO_TARGET_DIR", str(REPO / "target"))).resolve()
    for variant in ("baseline", "inline"):
        with tempfile.TemporaryDirectory(prefix="build-", dir=WORK) as temporary:
            tree = Path(temporary) / variant
            subprocess.run(["git", "worktree", "add", "--quiet", "--detach", str(tree), revision],
                           cwd=REPO, check=True)
            try:
                if variant == "baseline":
                    path = tree / "slatedb/src/sst_iter.rs"
                    path.write_text(baseline_policy(path.read_text()))
                env = os.environ.copy()
                env["CARGO_TARGET_DIR"] = str(target)
                log = WORK / f"build-{variant}.jsonl"
                with log.open("w") as output:
                    result = subprocess.run(
                        ["cargo", "test", "--locked", "--release", "-p", "slatedb", "--lib",
                         "issue2166_", "--no-run", "--message-format=json"],
                        cwd=tree, env=env, stdout=output,
                    )
                messages = [json.loads(line) for line in log.read_text().splitlines()]
                if result.returncode:
                    for message in messages:
                        if message.get("reason") == "compiler-message":
                            print(message["message"].get("rendered", ""))
                    raise SystemExit(result.returncode)
                executable = next(item["executable"] for item in messages
                                  if item.get("reason") == "compiler-artifact"
                                  and item.get("executable") and item["target"]["name"] == "slatedb")
                shutil.copy2(executable, WORK / f"{variant}-tests")
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(tree)],
                               cwd=REPO, check=True)
    print(f"Saved both binaries under {WORK}")


def unpack():
    manifest = json.loads((HERE / "results/archive-manifest.json").read_text())
    archive = HERE / "results/raw-samples.tar.gz"
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == manifest["archive_sha256"]
    expected = {item["path"]: item for item in manifest["files"]}
    with tarfile.open(archive) as tar:
        for member in tar:
            if not member.isfile() or member.name not in expected:
                raise ValueError("The archive contains an unexpected entry.")
            destination = (WORK / member.name).resolve()
            if not destination.is_relative_to(WORK.resolve()):
                raise ValueError("The archive path leaves the output directory.")
            payload = tar.extractfile(member).read()
            assert hashlib.sha256(payload).hexdigest() == expected[member.name]["sha256"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
    print(f"Extracted {len(expected)} recorded sample files under {WORK}")


def run_suite(args):
    single = args.suite in ("single", "recheck")
    output = WORK / ("single" if single else "extended")
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["SLATEDB_BENCH2166_EXT_MS"] = str(args.duration_ms)
    env["SLATEDB_BENCH2166_SCALE"] = "1"
    selected = [case for case in FOCUS_CASES if args.filter in case]
    candidates = selected if args.suite == "focus" else [case for case in MATRIX_CASES if args.filter in case]
    needs_s3 = not single and any(case.startswith("s3/") for case in candidates)
    if needs_s3:
        config = json.loads((WORK / "rustfs-private.json").read_text())
        for key in ("endpoint", "bucket", "access_key", "secret_key"):
            env["SLATEDB_RUSTFS_" + key.upper()] = config[key]
    rounds = 5 if args.suite == "focus" else 3
    groups = ("external", "worker") if args.suite == "single" else ("matrix",)
    if args.suite == "recheck":
        groups = ("batch64k", "tasks4")
    if args.suite == "focus":
        groups = selected
    for group in groups:
        for number in range(1, rounds + 1):
            variants = ("baseline", "inline") if number % 2 else ("inline", "baseline")
            for variant in variants:
                binary = WORK / f"{variant}-tests"
                if not binary.exists():
                    raise SystemExit("Build both comparison binaries first.")
                if args.suite == "single":
                    name = f"{group}-{variant}-{number}"
                    env["SLATEDB_BENCH2166_CALLER"] = group
                    env["SLATEDB_BENCH2166_FILTER"] = args.filter
                elif args.suite == "recheck":
                    name = f"recheck-{group}-{variant}-{number}"
                    env["SLATEDB_BENCH2166_CALLER"] = "worker"
                    env["SLATEDB_BENCH2166_FILTER"] = f"db/warm/8sst/{group}"
                    env["SLATEDB_BENCH2166_SCALE"] = "3"
                elif args.suite == "focus":
                    name = f"focus-{group.replace('/', '-')}-{variant}-{number}"
                    env["SLATEDB_BENCH2166_EXT_FILTER"] = group
                else:
                    name = f"{variant}-{number}"
                    env["SLATEDB_BENCH2166_EXT_FILTER"] = args.filter
                env["SLATEDB_BENCH2166_OUTPUT"] = str(output / f"{name}.json")
                with (output / f"{name}.log").open("w") as log:
                    subprocess.run([str(binary), "--ignored", "--exact",
                                    SINGLE_TEST if single else CONCURRENT_TEST, "--nocapture"],
                                   cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   check=True, timeout=180)
                print(name, flush=True)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build")
    sub.add_parser("unpack")
    run = sub.add_parser("run")
    run.add_argument("--suite", choices=("single", "recheck", "matrix", "focus"), required=True)
    run.add_argument("--filter", default="")
    run.add_argument("--duration-ms", type=int, default=2000)
    args = parser.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    if args.command == "build":
        build()
    elif args.command == "unpack":
        unpack()
    else:
        run_suite(args)


if __name__ == "__main__":
    main()
