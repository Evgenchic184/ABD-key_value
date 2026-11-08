"""One pull worker per replica. HTTP work never holds engine locks."""

from datetime import datetime, timezone
from threading import Event, Lock, Thread

import httpx

from kvdb.replication.models import Operation, integer


class ReplicationWorker:
    def __init__(self, engine, config):
        self.engine, self.config = engine, config
        self.stop_event, self.lock = Event(), Lock()
        self.transfer_lock, self.pause_event = Lock(), Event()
        self.info = {"state": "connecting", "last_error": None, "leader_last_seq": None,
                     "last_contact": None, "lag_stale": True,
                     "wal_operations": 0, "snapshots_installed": 0, "last_transfer": None}
        self.thread = Thread(target=self.run, name="replication", daemon=True)

    def update(self, **fields):
        with self.lock:
            self.info.update(fields)

    def status(self):
        with self.lock:
            result = self.info.copy()
        result["paused"] = self.pause_event.is_set()
        local = self.engine.status()["last_seq"]
        known = result["leader_last_seq"]
        result["lag"] = max(0, known - local) if known is not None else None
        return result

    def set_paused(self, paused: bool):
        # Returning from pause acknowledges that no transfer is still in flight.
        with self.transfer_lock:
            if self.thread.ident is not None and not self.thread.is_alive():
                raise RuntimeError("Replication worker stopped; restart the node")
            if paused:
                self.pause_event.set()
            else:
                self.pause_event.clear()
            self.update(state="paused" if paused else "connecting")
        return self.status()

    def start(self):
        self.thread.start()

    def close(self):
        self.stop_event.set()
        # Finish in-flight I/O before the server closes its engine.
        self.thread.join()

    def checked(self, response):
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict) or raw.get("group_id") != self.engine.group_id:
            raise ValueError("replication group mismatch")
        integer(raw.get("last_seq"), "last_seq")
        self.update(leader_last_seq=raw["last_seq"],
                    last_contact=datetime.now(timezone.utc).isoformat(),
                    lag_stale=False, last_error=None)
        return raw

    def snapshot(self, client):
        self.update(state="snapshot")
        raw = self.checked(client.get("/internal/replication/snapshot"))
        self.engine.install_snapshot(raw)
        with self.lock:
            self.info["snapshots_installed"] += 1
            self.info["last_transfer"] = "snapshot"

    def tick(self, client):
        if self.pause_event.is_set():
            # Observe the leader position without transferring or applying data.
            self.checked(client.get("/internal/replication/status"))
            self.update(state="paused")
            return False
        if not self.engine.status()["ready"]:
            if self.engine._failed:
                raise ValueError("local engine failed; restart required")
            self.snapshot(client)
        position = self.engine.status()["last_seq"]
        response = client.get("/internal/replication/changes",
                              params={"after": position, "limit": 1000})
        if response.status_code == 409:
            if response.json().get("code") != "snapshot_required":
                raise ValueError(f"replication conflict: {response.text}")
            self.snapshot(client)
            return True
        raw = self.checked(response)
        if raw["last_seq"] < position:
            raise ValueError("leader is behind replica")
        operations = raw.get("operations")
        if not isinstance(operations, list) or len(operations) > 1000:
            raise ValueError("invalid operation batch")
        expected = position + 1
        for item in operations:
            operation = Operation.parse(item)
            if operation.seq != expected or operation.seq > raw["last_seq"]:
                raise ValueError("invalid batch sequence")
            expected += 1
        if not operations and raw["last_seq"] != position:
            raise ValueError("missing operations in batch")
        self.update(state="wal" if operations else "following")
        for item in operations:
            if self.stop_event.is_set():
                return False
            self.engine.apply_replication(item)
            with self.lock:
                self.info["wal_operations"] += 1
                self.info["last_transfer"] = "wal"
        return bool(operations)

    def run(self):
        with httpx.Client(base_url=self.config.leader_url.rstrip("/"),
                          timeout=self.config.timeout, trust_env=False) as client:
            while not self.stop_event.is_set():
                try:
                    with self.transfer_lock:
                        more = self.tick(client)
                    if more:
                        continue
                except (httpx.HTTPError, OSError) as exc:
                    self.update(state="paused" if self.pause_event.is_set() else "disconnected",
                                last_error=str(exc), lag_stale=True)
                    if self.engine._failed:
                        self.update(state="error")
                        return
                except (ValueError, RuntimeError) as exc:
                    self.update(state="error", last_error=str(exc), lag_stale=True)
                    return
                self.stop_event.wait(self.config.interval)
