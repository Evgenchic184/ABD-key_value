"""ZooKeeper election and discovery for an asynchronous replication group."""

from datetime import datetime, timezone
import json
from threading import Event
from uuid import uuid4

import httpx
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NoNodeError, NodeExistsError
from kazoo.retry import KazooRetry

from kvdb.replication.models import SnapshotState
from kvdb.replication.worker import ReplicationWorker


class ZooKeeperCoordinator(ReplicationWorker):
    def __init__(self, engine, config):
        super().__init__(engine, config)
        self.connected = Event()
        self.authority = Event()
        self.root = f"/kvdb/{config.group_id}"
        self.leader_path = self.root + "/leader"
        self.member_path = self.root + "/members/" + config.node_id
        self.candidate = None
        self.candidate_prefix = uuid4().hex + "-candidate-"
        self.leader = None
        self.zk = KazooClient(
            hosts=config.zookeeper_hosts, timeout=config.session_timeout,
            connection_retry=KazooRetry(max_tries=-1, max_delay=1),
            command_retry=KazooRetry(max_tries=1),
        )
        self.zk.add_listener(self.connection_changed)
        self.engine.authorize = self.require_leadership
        self.update(coordination="LOST", node_id=config.node_id, leader_id=None,
                    leader_url=None, leader_epoch=None)

    def connection_changed(self, state):
        # Kazoo invokes listeners on its own thread. Do not block it on disk I/O.
        if state == KazooState.CONNECTED:
            self.connected.set()
        else:
            self.connected.clear()
            self.authority.clear()
        self.update(coordination=state)

    def require_leadership(self):
        if not self.connected.is_set() or not self.authority.is_set():
            raise RuntimeError("Leadership is unavailable while ZooKeeper is disconnected or electing")
        try:
            self.zk.sync(self.leader_path)
            raw, stat = self.zk.get(self.leader_path)
            leader = json.loads(raw)
            valid = (stat.ephemeralOwner == self.zk.client_id[0]
                     and stat.czxid == self.engine.epoch
                     and leader.get("node_id") == self.config.node_id
                     and leader.get("ready") is True)
        except (KazooException, ValueError) as exc:
            self.authority.clear()
            raise RuntimeError("Cannot verify leadership with ZooKeeper") from exc
        if not valid or not self.connected.is_set():
            self.authority.clear()
            raise RuntimeError("Leadership was revoked")

    def status(self):
        result = super().status()
        result["writable"] = self.authority.is_set() and self.connected.is_set()
        return result

    def set_paused(self, paused):
        with self.transfer_lock:
            if self.thread.ident is not None and not self.thread.is_alive():
                raise RuntimeError("Replication worker stopped; restart the node")
            if self.engine.role == "leader":
                raise ValueError("Only a replica can pause synchronization")
            if paused:
                self.pause_event.set()
                if self.connected.is_set():
                    self.withdraw()
            else:
                self.pause_event.clear()
            self.update(state="paused" if paused else "connecting")
        return self.status()

    def withdraw(self):
        if self.candidate:
            try:
                self.zk.delete(self.candidate)
            except NoNodeError:
                pass
            self.candidate = None

    def register(self):
        self.zk.ensure_path(self.root + "/members")
        self.zk.ensure_path(self.root + "/election")
        payload = json.dumps({"node_id": self.config.node_id,
                              "url": self.config.advertise_url.rstrip("/")}).encode()
        try:
            self.zk.create(self.member_path, payload, ephemeral=True)
        except NodeExistsError:
            _, stat = self.zk.get(self.member_path)
            if stat.ephemeralOwner != self.zk.client_id[0]:
                raise ValueError("node_id already belongs to another live process")

    def nominate(self):
        if self.candidate:
            stat = self.zk.exists(self.candidate)
            if stat and stat.ephemeralOwner == self.zk.client_id[0]:
                return
            self.candidate = None
        # Recover an uncertain create response using this process's UUID.
        children = self.zk.get_children(self.root + "/election")
        existing = [child for child in children if child.startswith(self.candidate_prefix)]
        if existing:
            self.candidate = self.root + "/election/" + existing[0]
        else:
            self.candidate = self.zk.create(
                self.root + "/election/" + self.candidate_prefix,
                self.config.node_id.encode(), ephemeral=True, sequence=True)

    def discover(self):
        try:
            raw, stat = self.zk.get(self.leader_path)
        except NoNodeError:
            return None
        leader = json.loads(raw)
        leader["epoch"] = stat.czxid
        self.leader = leader
        self.update(leader_id=leader["node_id"], leader_url=leader["url"],
                    leader_epoch=stat.czxid)
        return leader

    def freshest_snapshot(self, client):
        best = self.engine.export_snapshot()
        for member in self.zk.get_children(self.root + "/members"):
            if member == self.config.node_id:
                continue
            try:
                raw, _ = self.zk.get(self.root + "/members/" + member)
                url = json.loads(raw)["url"]
                response = client.get(url + "/internal/replication/local-snapshot")
                response.raise_for_status()
                candidate = response.json()
                state = SnapshotState.parse(candidate)
                if state.group_id != self.engine.group_id:
                    raise ValueError("peer belongs to another group")
                if state.initialized and (state.epoch, state.last_seq) > (best.get("epoch", 0), best["last_seq"]):
                    best = candidate
            except (httpx.HTTPError, NoNodeError):
                continue
        return best

    def elect(self, client):
        self.nominate()
        children = self.zk.get_children(self.root + "/election")
        first = min(children, key=lambda child: int(child.rsplit("-", 1)[1]))
        if not self.candidate.endswith("/" + first):
            return
        payload = {"node_id": self.config.node_id, "url": self.config.advertise_url.rstrip("/"),
                   "ready": False}
        try:
            self.zk.create(self.leader_path, json.dumps(payload).encode(), ephemeral=True)
        except NodeExistsError:
            return
        raw, stat = self.zk.get(self.leader_path)
        if stat.ephemeralOwner != self.zk.client_id[0] or json.loads(raw).get("node_id") != self.config.node_id:
            raise RuntimeError("Leader session changed before promotion")
        self.update(state="promoting")
        best = self.freshest_snapshot(client)
        if best.get("epoch", 0) >= stat.czxid:
            raise ValueError("ZooKeeper history is older than database epochs; restore coordination data")
        local = self.engine.status()
        if (best.get("epoch", 0), best["last_seq"]) > (local["epoch"], local["last_seq"]):
            self.engine.install_snapshot(best, allow_new_epoch=True)
        self.engine.promote(stat.czxid)
        payload["ready"] = True
        # A lost session cannot publish a leader owned by another session.
        transaction = self.zk.transaction()
        transaction.check(self.candidate, version=0)
        transaction.check(self.leader_path, version=stat.version)
        transaction.set_data(self.leader_path, json.dumps(payload).encode(), version=stat.version)
        results = transaction.commit()
        if any(isinstance(result, Exception) for result in results):
            raise RuntimeError("Election authority expired during promotion")
        self.authority.set()
        self.require_leadership()
        self.discover()
        self.update(state="leader", last_error=None, lag_stale=False,
                    leader_last_seq=self.engine.last_seq)

    def checked(self, response):
        raw = super().checked(response)
        if not self.leader or raw.get("epoch", 0) != self.leader["epoch"]:
            raise RuntimeError("leader changed during replication")
        return raw

    def snapshot(self, client):
        self.update(state="snapshot")
        raw = self.checked(client.get("/internal/replication/snapshot"))
        self.engine.install_snapshot(raw, allow_new_epoch=True)
        with self.lock:
            self.info["snapshots_installed"] += 1
            self.info["last_transfer"] = "snapshot"

    def coordinate(self, client):
        if not self.connected.is_set():
            self.engine.set_role("replica")
            self.update(state="disconnected", lag_stale=True)
            return False
        self.register()
        leader = self.discover()
        if leader and leader["node_id"] == self.config.node_id:
            if self.authority.is_set():
                self.require_leadership()
                self.update(state="leader", leader_last_seq=self.engine.last_seq,
                            last_error=None, lag_stale=False,
                            last_contact=datetime.now(timezone.utc).isoformat())
                return False
            # Reconnection after SUSPENDED always starts a fresh leadership epoch.
            _, stat = self.zk.get(self.leader_path)
            if stat.ephemeralOwner == self.zk.client_id[0]:
                self.zk.delete(self.leader_path, version=stat.version)
            leader = None
        self.authority.clear()
        self.engine.set_role("replica")
        if self.pause_event.is_set():
            self.withdraw()
        elif self.engine.status()["ready"]:
            self.nominate()
        if leader is None:
            self.update(state="paused" if self.pause_event.is_set() else "electing",
                        leader_id=None, leader_url=None, leader_epoch=None, lag_stale=True)
            if not self.pause_event.is_set() and self.engine.status()["ready"]:
                self.elect(client)
            return False
        if not leader.get("ready"):
            self.update(state="electing", lag_stale=True)
            return False
        with httpx.Client(base_url=leader["url"], timeout=self.config.timeout, trust_env=False) as source:
            if not self.pause_event.is_set() and self.engine.epoch != leader["epoch"]:
                self.snapshot(source)
            return self.tick(source)

    def run(self):
        self.zk.start_async()
        try:
            with httpx.Client(timeout=self.config.timeout, trust_env=False) as client:
                while not self.stop_event.is_set():
                    try:
                        with self.transfer_lock:
                            more = self.coordinate(client)
                        if more:
                            continue
                    except (httpx.HTTPError, KazooException, OSError, RuntimeError) as exc:
                        self.authority.clear()
                        self.update(state="disconnected", last_error=str(exc), lag_stale=True)
                        if self.engine._failed:
                            self.update(state="error")
                            break
                        self.engine.set_role("replica")
                    except ValueError as exc:
                        self.authority.clear()
                        self.engine.set_role("replica")
                        self.update(state="error", last_error=str(exc), lag_stale=True)
                        break
                    self.stop_event.wait(self.config.interval)
        finally:
            self.authority.clear()
            self.zk.stop()
            self.zk.close()
