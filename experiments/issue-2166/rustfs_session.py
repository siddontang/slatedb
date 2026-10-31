import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent / ".runs"
ROOT.mkdir(parents=True, exist_ok=True)
PRIVATE = ROOT / "rustfs-private.json"
parser = argparse.ArgumentParser()
parser.add_argument("action", choices=("start", "stop"))
action = parser.parse_args().action
if action == "start":
    if PRIVATE.exists():
        raise SystemExit("A local RustFS session record already exists.")
    executable = shutil.which("rustfs")
    subprocess.run([executable, "--version"], check=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = {
        "endpoint": f"http://127.0.0.1:{port}", "bucket": "slatedb-2166-test",
        "access_key": secrets.token_hex(10), "secret_key": secrets.token_hex(24),
        "data_dir": tempfile.mkdtemp(prefix="slatedb-2166-rustfs-"),
    }
    env = {k: v for k, v in os.environ.items() if not k.startswith("RUSTFS_")}
    env.update({"RUSTFS_ACCESS_KEY": config["access_key"], "RUSTFS_SECRET_KEY": config["secret_key"],
                "RUSTFS_CONSOLE_ENABLE": "false", "RUSTFS_REGION": "us-east-1", "RUST_LOG": "warn"})
    with (ROOT / "rustfs.log").open("w") as log:
        process = subprocess.Popen([executable, "server", "--address", f"127.0.0.1:{port}", config["data_dir"]],
                                   env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    config["pid"] = process.pid
    PRIVATE.write_text(json.dumps(config))
    PRIVATE.chmod(0o600)
    env.update({"AWS_ACCESS_KEY_ID": config["access_key"], "AWS_SECRET_ACCESS_KEY": config["secret_key"],
                "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": "/dev/null",
                "AWS_SHARED_CREDENTIALS_FILE": "/dev/null", "AWS_PAGER": ""})
    for _ in range(30):
        if process.poll() is not None:
            raise SystemExit("RustFS stopped. Read the local log.")
        result = subprocess.run([shutil.which("aws"), "--endpoint-url", config["endpoint"], "--region", "us-east-1",
                                 "--cli-connect-timeout", "2", "--cli-read-timeout", "3",
                                 "s3api", "create-bucket", "--bucket", config["bucket"]],
                                env=env, capture_output=True)
        if result.returncode == 0:
            print(json.dumps({k: config[k] for k in ("endpoint", "bucket", "pid", "data_dir")}))
            break
        time.sleep(1)
    else:
        raise SystemExit("RustFS did not accept bucket creation.")
else:
    config = json.loads(PRIVATE.read_text())
    command = subprocess.run(["ps", "-p", str(config["pid"]), "-o", "command="], capture_output=True, text=True).stdout
    if command:
        if config["data_dir"] not in command or "rustfs" not in command:
            raise SystemExit("The recorded process does not match this test session.")
        os.kill(config["pid"], signal.SIGTERM)
        for _ in range(50):
            result = subprocess.run(["ps", "-p", str(config["pid"]), "-o", "stat="], capture_output=True, text=True)
            if not result.stdout.strip() or result.stdout.strip().startswith("Z"):
                break
            time.sleep(0.1)
        else:
            raise SystemExit("RustFS did not stop. The test data remains available.")
    data = Path(config["data_dir"]).resolve()
    assert data.parent == Path(tempfile.gettempdir()).resolve()
    assert data.name.startswith("slatedb-2166-rustfs-")
    shutil.rmtree(data)
    PRIVATE.unlink()
    print("Stopped RustFS and removed its temporary test data and credentials.")
