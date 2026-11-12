"""Threaded demo workload. The only connection to KVDB is its HTTP API."""

import base64
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from threading import Condition, Event, Thread
import time
from urllib.parse import quote
from uuid import uuid4

import httpx


class Demo:
    def __init__(self, nodes, *, users=3, interval=1.0, log_dir=Path("reports/demo"), autostart=True):
        self.nodes, self.users, self.interval = nodes, users, interval
        self.run_id = uuid4().hex[:12]
        self.prefix = f"demo/{self.run_id}"
        self.control_key = f"{self.prefix}/control"
        self.control_value = base64.b64encode(f"control:{self.run_id}".encode()).decode()
        self.stop = Event()
        self.condition = Condition()
        self.running, self.initialized, self.active = autostart, False, 0
        self.events = deque(maxlen=2000)
        self.counter, self.cursor, self.rotation = Counter(), 0, 0
        self.node_status, self.history = {}, {name: deque(maxlen=300) for name in nodes}
        self.touched = {self.control_key}
        self.control = None
        self.values = {}
        self.verification = {"state": "idle"}
        self.threads = []
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{self.run_id}.jsonl"
        self.log = self.log_path.open("a", encoding="utf-8", buffering=1)
        self.client = httpx.Client(timeout=2.0, trust_env=False)

    def spawn(self, target, *args):
        thread = Thread(target=target, args=args, daemon=True)
        self.threads.append(thread)
        thread.start()

    def start(self):
        for name in self.nodes:
            self.spawn(self.monitor, name)
        self.spawn(self.bootstrap)
        self.spawn(self.control_loop)
        for number in range(1, self.users + 1):
            self.spawn(self.user, number)

    def close(self):
        self.stop.set()
        with self.condition:
            self.condition.notify_all()
        for thread in self.threads:
            thread.join()
        self.client.close()
        self.log.close()

    def emit(self, **event):
        with self.condition:
            self.cursor += 1
            event.update(id=self.cursor, time=datetime.now(timezone.utc).isoformat())
            self.events.append(event)
            if event.get("kind") == "request":
                category = "read" if event["method"] == "GET" else "write" if event["method"] == "PUT" else "delete"
                self.counter[f"{category}_{'ok' if event['ok'] else 'error'}"] += 1
            line = json.dumps(event, ensure_ascii=False)
            self.log.write(line + "\n")
        logger = logging.getLogger("kvdb.demo")
        if event.get("kind") == "request":
            raw_value = event.get("sent_value") if event["method"] == "PUT" else (event.get("body") or {}).get("value")
            text = ""
            if raw_value is not None:
                try:
                    text = " value=" + repr(base64.b64decode(raw_value, validate=True).decode("utf-8"))
                except (ValueError, UnicodeError):
                    text = " value=<binary>"
            logger.info("%s %-9s %s → %-8s %-6s %s%s → %s (%.1f ms)%s",
                        event["time"], event.get("user", ""), event.get("target", "node"), event["node"], event["method"],
                        event["key"], text, event["status"] or "NETWORK", event["duration_ms"],
                        " UNKNOWN" if event["outcome"] == "unknown" else "")
        else:
            logger.info("%s %s %s", event["time"], event.get("node", "system"), event.get("message", ""))
        return event

    def leader_name(self):
        with self.condition:
            candidates = [(info.get("epoch", 0), name) for name, info in self.node_status.items()
                          if info.get("available") and info.get("role") == "leader"
                          and info.get("writable", True)]
            managed = any("coordination" in info for info in self.node_status.values())
        if candidates:
            return max(candidates)[1]
        return None if managed else next(iter(self.nodes))

    def leader_url(self):
        return self.nodes[self.leader_name() or next(iter(self.nodes))]

    def request(self, node, method, key, *, value=None, user="control", target="node"):
        if target == "leader":
            node = self.leader_name()
        started = time.monotonic()
        status, body, error = None, None, None
        url = self.nodes.get(node)
        with self.condition:
            node_role = self.node_status.get(node, {}).get("role")
        if url is None:
            status, body = 503, {"detail": "Текущий лидер ещё не определён"}
        else:
            try:
                response = self.client.request(method, url + "/kv/" + quote(key, safe=""),
                                               **({"json": {"value": value}} if method == "PUT" else {}))
                status = response.status_code
                try:
                    body = response.json()
                except ValueError:
                    body = {"detail": response.text[:300]}
            except httpx.HTTPError as exc:
                error = f"{type(exc).__name__}: {exc}"
        ok = status == 200 or (method == "GET" and status == 404)
        outcome = "confirmed" if ok else "unknown" if method != "GET" and (status is None or status >= 500) else "failed"
        if url is None:
            outcome = "failed"
        return self.emit(kind="request", user=user, node=node, target=target, node_role=node_role,
                         url=url, method=method, key=key, status=status, ok=ok, outcome=outcome,
                         body=body, error=error, sent_value=value,
                         duration_ms=round((time.monotonic() - started) * 1000, 1))

    def read(self, key, user):
        leader = self.leader_name() or next(iter(self.nodes))
        replicas = [name for name in self.nodes if name != leader]
        with self.condition:
            index = self.rotation % max(1, len(replicas))
            self.rotation += 1
        ordered = replicas[index:] + replicas[:index] + [leader]
        result = None
        for name in ordered:
            result = self.request(name, "GET", key, user=user)
            if result["status"] is not None and result["status"] != 503:
                break
        return result

    def bootstrap(self):
        while not self.stop.is_set():
            result = self.request(None, "PUT", self.control_key,
                                  value=self.control_value, user="bootstrap", target="leader")
            if result["status"] == 200:
                break
            self.stop.wait(1)
        while not self.stop.is_set():
            ready = True
            for name in self.nodes:
                result = self.request(name, "GET", self.control_key, user="bootstrap")
                ready &= result["status"] == 200 and result["body"].get("value") == self.control_value
            if ready:
                with self.condition:
                    self.initialized = True
                    self.condition.notify_all()
                self.emit(kind="system", message="Control key replicated; workload ready")
                return
            self.stop.wait(1)

    def user(self, number):
        cycle = 0
        key, temporary = f"user-{number}", f"user-{number}-temp"
        # Continue a visible counter across web restarts rather than resetting it.
        while not self.stop.is_set():
            with self.condition:
                self.condition.wait_for(lambda: self.stop.is_set() or self.initialized)
            if self.stop.is_set():
                return
            try:
                response = self.client.get(self.leader_url() + "/kv/" + key)
                if response.status_code != 404:
                    response.raise_for_status()
                    cycle = max(0, int(base64.b64decode(response.json()["value"]).decode()))
                break
            except httpx.HTTPError:
                self.stop.wait(1)
            except (ValueError, KeyError, UnicodeError):
                break
        while not self.stop.is_set():
            cycle += 1
            value = base64.b64encode(str(cycle).encode()).decode()
            # The main counter survives DELETE so lag remains visible throughout a cycle.
            operations = (("PUT", key), ("GET", key), ("PUT", temporary),
                          ("GET", temporary), ("DELETE", temporary), ("GET", temporary))
            for method, operation_key in operations:
                with self.condition:
                    self.condition.wait_for(lambda: self.stop.is_set() or (self.running and self.initialized))
                    if self.stop.is_set():
                        return
                    self.active += 1
                    self.touched.add(operation_key)
                try:
                    if method == "GET":
                        self.read(operation_key, f"user-{number}")
                    else:
                        self.request(None, method, operation_key, value=value, user=f"user-{number}", target="leader")
                finally:
                    with self.condition:
                        self.active -= 1
                        self.condition.notify_all()
                self.stop.wait(self.interval)

    def control_loop(self):
        while not self.stop.is_set():
            if self.initialized:
                result = self.read(self.control_key, "control")
                with self.condition:
                    self.control = {"ok": result["status"] == 200 and result["body"].get("value") == self.control_value,
                                    "node": result["node"], "time": result["time"], "status": result["status"]}
            self.stop.wait(1)

    def monitor(self, name):
        while not self.stop.is_set():
            try:
                response = self.client.get(self.nodes[name] + "/replication/status")
                response.raise_for_status()
                raw = response.json()
                if not isinstance(raw, dict) or type(raw.get("last_seq")) is not int:
                    raise ValueError("node is not running in cluster mode")
                observed = {}
                for number in range(1, self.users + 1):
                    key = f"user-{number}"
                    value_response = self.client.get(self.nodes[name] + "/kv/" + key)
                    if value_response.status_code == 200:
                        observed[key] = {"status": 200, "value": value_response.json()["value"]}
                    elif value_response.status_code == 404:
                        observed[key] = {"status": 404, "value": None}
                    else:
                        observed[key] = {"status": value_response.status_code, "value": None}
                raw.update(available=True, last_success=datetime.now(timezone.utc).isoformat())
                with self.condition:
                    previous = self.node_status.get(name, {})
                    self.node_status[name] = raw
                    self.values[name] = observed
                    self.history[name].append({"time": time.time(), "seq": raw["last_seq"]})
                if previous.get("role") != raw.get("role") or previous.get("epoch") != raw.get("epoch"):
                    self.emit(kind="election", node=name,
                              message=f"Role {raw.get('role')}, epoch {raw.get('epoch', 0)}, leader {raw.get('leader_id', 'static')}")
                if not previous.get("available"):
                    self.emit(kind="node", node=name, message="Node connected")
            except (httpx.HTTPError, ValueError) as exc:
                with self.condition:
                    previous = self.node_status.get(name, {})
                    self.node_status[name] = {**previous, "available": False, "error": str(exc)}
                    self.history[name].append({"time": time.time(), "seq": None})
                if previous.get("available", True):
                    self.emit(kind="node", node=name, message="Node disconnected", error=str(exc))
            self.stop.wait(1)

    def state(self, after=0):
        with self.condition:
            leader_name = self.leader_name()
            leader = self.node_status.get(leader_name, {})
            nodes = {}
            for name, info in self.node_status.items():
                info = info.copy()
                same_epoch = info.get("epoch", 0) == leader.get("epoch", 0)
                info["epoch_mismatch"] = not same_epoch
                info["display_lag_stale"] = not leader.get("available") or not info.get("available") or not same_epoch
                seq = leader.get("last_seq")
                info["display_lag"] = max(0, seq - info["last_seq"]) if same_epoch and seq is not None and "last_seq" in info else None
                nodes[name] = info
            return {"leader_node": leader_name, "run_id": self.run_id, "running": self.running, "initialized": self.initialized,
                    "active": self.active, "nodes": nodes, "node_urls": self.nodes.copy(), "counters": dict(self.counter),
                    "events": [event for event in self.events if event["id"] > after],
                    "cursor": self.cursor, "history": {n: list(h) for n, h in self.history.items()},
                    "events_truncated": bool(self.events and after and after < self.events[0]["id"] - 1),
                    "control": self.control, "verification": self.verification.copy(),
                    "values": {name: {key: value.copy() for key, value in values.items()}
                               for name, values in self.values.items()},
                    "user_keys": [f"user-{n}" for n in range(1, self.users + 1)],
                    "log_path": str(self.log_path)}

    def set_running(self, running):
        with self.condition:
            if self.verification["state"] == "running":
                raise ValueError("verification in progress")
            self.running = running
            self.condition.notify_all()
        self.emit(kind="system", message="Workload started" if running else "Workload paused")

    def flush(self):
        node = self.leader_name()
        if node is None:
            raise ValueError("Текущий лидер ещё не определён")
        response = self.client.post(self.nodes[node] + "/flush")
        self.emit(kind="system", node=node, message="Leader flush", status=response.status_code)
        response.raise_for_status()
        return response.json()

    def replication_control(self, node, action):
        if node not in self.nodes or node == self.leader_name() or action not in ("pause", "resume"):
            raise ValueError("choose a configured replica and pause/resume")
        response = self.client.post(self.nodes[node] + "/internal/replication/" + action, timeout=30)
        response.raise_for_status()
        self.emit(kind="system", node=node, message=f"Synchronization {action}")
        return response.json()

    def manual(self, node, method, key, value, *, target="node"):
        if (target not in ("node", "leader") or (target == "node" and node not in self.nodes)
                or method not in ("GET", "PUT", "DELETE")):
            raise ValueError("invalid node or method")
        with self.condition:
            if self.verification["state"] == "running":
                raise ValueError("verification in progress")
            self.active += 1
            self.touched.add(key)
        try:
            encoded = base64.b64encode(value.encode("utf-8")).decode() if method == "PUT" else None
            return self.request(node, method, key, value=encoded, user="manual", target=target)
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def begin_check(self):
        with self.condition:
            if not self.initialized:
                raise ValueError("wait for control key initialization")
            if self.verification["state"] == "running":
                raise ValueError("verification already in progress")
            self.running = False
            self.verification = {"state": "running", "message": "Waiting for in-flight operations"}
        self.spawn(self.verify)

    def verify(self):
        deadline = time.monotonic() + 30
        try:
            with self.condition:
                while self.active:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or self.stop.is_set():
                        raise ValueError("in-flight requests did not finish")
                    self.condition.wait(min(remaining, 0.2))
                keys = sorted(self.touched)
            snapshot = self.client.get(self.leader_url() + "/internal/replication/snapshot")
            snapshot.raise_for_status()
            snapshot = snapshot.json()
            target = snapshot["last_seq"]
            epoch = snapshot.get("epoch", 0)
            with self.condition:
                self.verification["message"] = f"Waiting for seq {target}"
                self.verification["target_seq"] = target
            while True:
                if self.stop.is_set() or time.monotonic() >= deadline:
                    raise ValueError("nodes did not converge within 30 seconds")
                try:
                    positions = []
                    for url in self.nodes.values():
                        response = self.client.get(url + "/replication/status")
                        response.raise_for_status()
                        status = response.json()
                        positions.append(status.get("last_seq") == target and status.get("ready")
                                         and status.get("group_id") == snapshot["group_id"]
                                         and status.get("epoch", 0) == epoch)
                    if all(positions):
                        break
                except httpx.HTTPError:
                    pass
                self.stop.wait(0.2)

            def check_node(item):
                name, url = item
                for key in keys:
                    if self.stop.is_set() or time.monotonic() >= deadline:
                        raise ValueError("data verification exceeded 30 seconds")
                    response = self.client.get(url + "/kv/" + quote(key, safe=""))
                    expected = snapshot["data"].get(key)
                    if expected is None:
                        if response.status_code != 404:
                            raise ValueError(f"{name}: key should be absent: {key}")
                    elif response.status_code != 200 or response.json().get("value") != expected:
                        raise ValueError(f"{name}: value mismatch: {key}")
                response = self.client.get(url + "/replication/status")
                response.raise_for_status()
                final = response.json()
                if final["last_seq"] != target or final.get("epoch", 0) != epoch:
                    raise ValueError("external writes changed the verification position")

            if snapshot["data"].get(self.control_key) != self.control_value:
                raise ValueError("control key was modified")
            with ThreadPoolExecutor(max_workers=len(self.nodes)) as pool:
                list(pool.map(check_node, self.nodes.items()))
            result = {"state": "pass", "message": f"PASS: seq {target}; {len(keys)} keys on all nodes",
                      "target_seq": target, "keys_checked": len(keys)}
        except Exception as exc:
            result = {"state": "fail", "message": f"FAIL / incomplete: {exc}"}
        with self.condition:
            self.verification = result
        self.emit(kind="verification", **result)
