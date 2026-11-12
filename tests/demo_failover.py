"""Standalone Docker failover demonstration, run with python tests/demo_failover.py."""

import argparse
import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
HTTP = build_opener(ProxyHandler({}))
NODES = {"node1": "http://127.0.0.1:8000", "node2": "http://127.0.0.1:8001",
         "node3": "http://127.0.0.1:8002"}


def request(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with HTTP.open(req, timeout=3) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except ValueError:
            body = {"error": raw[:300]}
        return exc.code, body
    except (URLError, TimeoutError, OSError) as exc:
        return None, {"error": str(exc)}


def wait_for(description, predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            print(f"PASS  {description}", flush=True)
            return result
        time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {description}")


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--web-url", default="http://127.0.0.1:8080")
    parser.add_argument("--mode", choices=("stop", "kill"), default="stop")
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    web = args.web_url.rstrip("/")

    def state():
        status, body = request(web + "/api/state")
        return body if status == 200 else {}

    def leader_except(excluded=None):
        for name, url in NODES.items():
            if name == excluded:
                continue
            status, body = request(url + "/replication/status")
            if status == 200 and body.get("role") == "leader" and body.get("writable"):
                return name, body
        return None

    wait_for("web initialized", lambda: state().get("initialized"), args.timeout)
    request(web + "/api/action/pause", "POST")
    wait_for("in-flight workload completed", lambda: state().get("active") == 0, args.timeout)
    original, initial = wait_for("writable leader", leader_except, args.timeout)
    key = "failover-" + uuid4().hex[:8]

    def write(value):
        status, body = request(web + "/api/kv", "POST",
                               {"target": "leader", "method": "PUT", "key": key, "value": value})
        if status != 200 or body.get("status") != 200:
            raise RuntimeError(f"PUT failed: {status} {body}")
        print(f"PUT   {key} = {value!r} on {body['node']}", flush=True)
        return body

    write("before election")
    wait_for("initial record replicated", lambda: all(
        request(url + "/kv/" + key)[0] == 200 for url in NODES.values()), args.timeout)
    stopped = False
    try:
        print(f"{args.mode.upper()}  {original}, epoch {initial['epoch']}", flush=True)
        compose(args.mode, original)
        stopped = True
        survivor = next(name for name in NODES if name != original)
        status, body = request(web + "/api/kv", "POST",
                               {"node": survivor, "method": "GET", "key": key})
        if status != 200 or body.get("status") != 200:
            raise RuntimeError(f"Surviving replica read failed: {status} {body}")
        print(f"PASS  reading from {survivor} while {original} is down", flush=True)
        elected, elected_status = wait_for("new writable leader", lambda: leader_except(original), args.timeout)
        if elected_status["epoch"] <= initial["epoch"]:
            raise RuntimeError("New epoch did not increase")
        print(f"LEADER {elected}, epoch {elected_status['epoch']}", flush=True)
        wait_for("web discovered elected leader", lambda: state().get("leader_node") == elected, args.timeout)
        write("after election")
    finally:
        if stopped:
            compose("start", original)
    wait_for("old leader returned as replica", lambda: (
        (body := request(NODES[original] + "/replication/status"))[0] == 200
        and body[1].get("role") == "replica"
        and body[1].get("epoch") == elected_status["epoch"]), args.timeout)
    request(web + "/api/action/check", "POST")
    def verified():
        result = state().get("verification", {})
        if result.get("state") == "fail":
            raise RuntimeError(result.get("message"))
        return result if result.get("state") == "pass" else None
    result = wait_for("convergence after leader return", verified, args.timeout)
    print(result["message"], flush=True)
    print("Workload is paused. Resume it with the web button.", flush=True)


if __name__ == "__main__":
    main()
