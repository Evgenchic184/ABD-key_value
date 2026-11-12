"""Regression checks for epochs and ZooKeeper session authority."""

import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLoss
import pytest

from kvdb import FileStorage, FileWAL
from kvdb.replication.config import ReplicationConfig
from kvdb.replication.coordinator import ZooKeeperCoordinator
from kvdb.replication.engine import ReplicatedEngine
from kvdb.replication.models import ReplicationConflict, SnapshotState


def engine(path):
    return ReplicatedEngine(FileStorage(path / "snapshot.json"), FileWAL(path / "wal.bin"),
                            role="leader", group_id="test")


def test_epoch_persists_in_snapshot_and_wal(tmp_path):
    db = engine(tmp_path)
    db.promote(10)
    db.set("a", b"new")
    record = db.changes(0, 10)["operations"][0]
    assert record["epoch"] == 10
    recovered = engine(tmp_path)
    assert recovered.epoch == 10 and recovered.get("a") == b"new"
    recovered.set_role("replica")
    with pytest.raises(ReplicationConflict, match="epoch_mismatch"):
        recovered.apply_replication({**record, "epoch": 9})
    with pytest.raises(ValueError):
        recovered.promote(10)


def test_new_epoch_replaces_divergent_data_at_same_position(tmp_path):
    db = engine(tmp_path)
    db.promote(10)
    db.set("a", b"orphan")
    db.set_role("replica")
    db.install_snapshot(SnapshotState("test", 1, True, {"a": b"canonical"}, 20).wire(),
                        allow_new_epoch=True)
    assert db.last_seq == 1 and db.get("a") == b"canonical"
    with pytest.raises(ValueError):
        db.install_snapshot(SnapshotState("test", 1, True, {"a": b"stale"}, 10).wire(),
                            allow_new_epoch=True)


def test_rollback_snapshot_crash_does_not_replay_old_epoch(tmp_path):
    db = engine(tmp_path)
    db.promote(10)
    db.set("orphan", b"one")
    db.set("orphan", b"two")
    db.set_role("replica")
    with patch.object(db._wal, "clear", side_effect=OSError("crash after replace")):
        with pytest.raises(OSError):
            db.install_snapshot(SnapshotState("test", 0, True, {}, 20).wire(), allow_new_epoch=True)
    recovered = engine(tmp_path)
    assert recovered.epoch == 20 and recovered.last_seq == 0
    assert recovered.get("orphan") is None
    assert recovered.changes(0, 100)["operations"] == []
    recovered.set("valid", b"new epoch")
    assert len(recovered.changes(0, 100)["operations"]) == 1


def coordinator(tmp_path):
    db = engine(tmp_path)
    config = ReplicationConfig("leader", "test", zookeeper_hosts="zk:2181",
                               node_id="n1", advertise_url="http://n1:8000")
    with patch("kvdb.replication.coordinator.KazooClient") as constructor:
        worker = ZooKeeperCoordinator(db, config)
    zk = constructor.return_value
    zk.client_id = (123, b"password")
    return db, worker, zk


def test_connection_loss_revokes_write_authority_immediately(tmp_path):
    db, worker, zk = coordinator(tmp_path)
    db.promote(10)
    worker.connection_changed(KazooState.CONNECTED)
    worker.authority.set()
    zk.get.return_value = (json.dumps({"node_id": "n1", "ready": True}).encode(),
                           SimpleNamespace(ephemeralOwner=123, czxid=10))
    db.set("a", b"ok")
    worker.connection_changed(KazooState.SUSPENDED)
    with pytest.raises(RuntimeError):
        db.set("a", b"blocked")
    assert db.get("a") == b"ok"
    worker.connection_changed(KazooState.CONNECTED)
    with pytest.raises(RuntimeError):
        db.set("a", b"still blocked")
    assert not worker.status()["writable"]


def test_leader_owner_and_epoch_are_checked_before_wal_append(tmp_path):
    db, worker, zk = coordinator(tmp_path)
    db.promote(10)
    worker.connection_changed(KazooState.CONNECTED)
    worker.authority.set()
    zk.get.return_value = (json.dumps({"node_id": "n2", "ready": True}).encode(),
                           SimpleNamespace(ephemeralOwner=456, czxid=11))
    with pytest.raises(RuntimeError):
        db.set("a", b"stale")
    assert db.last_seq == 0 and not list(db._wal.replay())
    worker.authority.set()
    zk.sync.side_effect = ConnectionLoss()
    with pytest.raises(RuntimeError):
        db.set("a", b"unverified")
    assert db.last_seq == 0


def test_paused_replica_withdraws_from_election(tmp_path):
    db, worker, zk = coordinator(tmp_path)
    db.set_role("replica")
    worker.connection_changed(KazooState.CONNECTED)
    worker.candidate = "/kvdb/test/election/candidate-0001"
    assert worker.set_paused(True)["paused"]
    zk.delete.assert_called_once_with("/kvdb/test/election/candidate-0001")
    assert worker.candidate is None
    worker.set_paused(False)
    assert not worker.status()["paused"]


def test_election_selects_freshest_reachable_snapshot(tmp_path):
    db, worker, zk = coordinator(tmp_path)
    db.set_role("replica")
    zk.get_children.return_value = ["n1", "n2", "n3"]
    zk.get.side_effect = [(b'{"url":"http://n2"}', None), (b'{"url":"http://n3"}', None)]
    client = Mock()
    client.get.return_value.json.side_effect = [
        SnapshotState("test", 8, True, {"a": b"old"}, 1).wire(),
        SnapshotState("test", 3, True, {"a": b"new epoch"}, 2).wire(),
    ]
    best = worker.freshest_snapshot(client)
    assert best["epoch"] == 2 and best["last_seq"] == 3


def test_authority_loss_after_fsync_leaves_unknown_local_result(tmp_path):
    db, worker, zk = coordinator(tmp_path)
    db.promote(10)
    worker.connection_changed(KazooState.CONNECTED)
    worker.authority.set()
    valid = (b'{"node_id":"n1","ready":true}', SimpleNamespace(ephemeralOwner=123, czxid=10))
    zk.get.side_effect = [valid, ConnectionLoss()]
    with pytest.raises(RuntimeError):
        db.set("a", b"unknown")
    assert db.get("a") == b"unknown" and db.last_seq == 1
    assert not worker.status()["writable"]
    db.set_role("replica")
    db.install_snapshot(SnapshotState("test", 0, True, {}, 20).wire(), allow_new_epoch=True)
    assert db.get("a") is None


def test_web_routes_mutations_to_elected_node_and_compares_epochs(tmp_path):
    import httpx
    from kvdb.demo.service import Demo

    demo = Demo({"leader": "http://old", "replica1": "http://new", "replica2": "http://other"},
                log_dir=tmp_path, autostart=False)
    demo.client.close()
    requests = []
    def serve(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "ok"})
    demo.client = httpx.Client(transport=httpx.MockTransport(serve))
    try:
        demo.node_status = {
            "leader": {"role": "replica", "available": True, "epoch": 10, "last_seq": 50},
            "replica1": {"role": "leader", "available": True, "writable": True,
                         "epoch": 20, "last_seq": 40, "coordination": "CONNECTED"},
        }
        result = demo.request(None, "PUT", "a", value="dg==", target="leader")
        assert result["node"] == "replica1" and requests[-1].url.host == "new"
        direct = demo.manual("leader", "PUT", "a", "direct", target="node")
        assert direct["node"] == "leader" and requests[-1].url.host == "old"
        assert direct["target"] == "node" and direct["node_role"] == "replica"
        by_role = demo.manual(None, "GET", "a", "", target="leader")
        assert by_role["node"] == "replica1" and requests[-1].url.host == "new"
        state = demo.state()
        assert state["leader_node"] == "replica1"
        assert state["nodes"]["leader"]["epoch_mismatch"]
        assert state["nodes"]["leader"]["display_lag"] is None
        demo.replication_control("leader", "pause")
        assert requests[-1].url.host == "old"
        demo.node_status["replica1"]["writable"] = False
        assert demo.state()["leader_node"] is None
        count = len(requests)
        unavailable = demo.manual(None, "PUT", "a", "blocked", target="leader")
        assert unavailable["status"] == 503 and unavailable["outcome"] == "failed"
        assert unavailable["node"] is None and len(requests) == count
    finally:
        demo.close()
