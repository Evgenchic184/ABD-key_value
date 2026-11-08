"""Storage/protocol regression checks; the interactive demo is independent."""

from concurrent.futures import ThreadPoolExecutor
import json
from unittest.mock import patch

from fastapi.testclient import TestClient
import pytest

from kvdb import FileStorage, FileWAL, KVEngine
from kvdb._files import DirectoryLock
from kvdb.http.server import create_app
from kvdb.replication.config import ReplicationConfig
from kvdb.replication.engine import ReplicatedEngine
from kvdb.replication.models import Operation, ReadOnlyError, ReplicationConflict, SnapshotState


def engine(path, role="leader", group="test"):
    return ReplicatedEngine(FileStorage(path / "snapshot.json"), FileWAL(path / "wal.bin"),
                            role=role, group_id=group)


def test_concurrent_sequence_restart_flush_and_values(tmp_path):
    db = engine(tmp_path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda n: db.set(str(n), bytes([n])), range(64)))
    batch = db.changes(0, 1000)
    assert [o["seq"] for o in batch["operations"]] == list(range(1, 65))
    recovered = engine(tmp_path)
    assert recovered.status()["last_seq"] == 64
    assert recovered.get("63") == bytes([63])
    recovered.set("", b"")
    recovered.delete("missing")
    recovered.flush()
    recovered = engine(tmp_path)
    recovered.set("binary/ключ", bytes(range(256)))
    assert recovered.status()["last_seq"] == 67
    assert recovered.get("binary/ключ") == bytes(range(256))
    with pytest.raises(ReplicationConflict, match="snapshot_required"):
        recovered.changes(0, 10)


def test_replay_saved_snapshot_with_uncleared_wal(tmp_path):
    db = engine(tmp_path)
    db.set("a", b"old")
    db.set("a", b"new")
    db._storage.save_state(SnapshotState.parse(db.export_snapshot()))
    db = engine(tmp_path)
    assert db.get("a") == b"new"
    assert db.last_seq == 2
    db.set("b", b"next")
    assert db.last_seq == 3


def test_replica_duplicate_gap_and_snapshot_rollback(tmp_path):
    leader = engine(tmp_path / "leader")
    replica = engine(tmp_path / "replica", "replica")
    with pytest.raises(RuntimeError):
        replica.get("a")
    with pytest.raises(ReadOnlyError):
        replica.set("a", b"x")
    replica.install_snapshot(leader.export_snapshot())
    leader.set("a", b"v")
    record = leader.changes(0, 10)["operations"][0]
    replica.apply_replication(record)
    replica.apply_replication(record)
    assert replica.last_seq == 1
    assert len(list(replica._wal.replay())) == 1
    with pytest.raises(ReplicationConflict):
        replica.apply_replication({**record, "seq": 3})
    with pytest.raises(ValueError):
        replica.install_snapshot(SnapshotState("test", 0, True, {}).wire())
    leader.delete("a")
    leader.flush()
    replica.install_snapshot(leader.export_snapshot())
    assert replica.get("a") is None
    reopened = engine(tmp_path / "replica", "replica")
    assert reopened.initialized and reopened.last_seq == 2


@pytest.mark.parametrize("phase", ["snapshot", "clear"])
def test_flush_failure_is_fail_closed_and_recovers(tmp_path, phase):
    db = engine(tmp_path)
    db.set("a", b"v")
    if phase == "snapshot":
        target, name = db._storage, "save_state"
    else:
        target, name = db._wal, "clear"
    with patch.object(target, name, side_effect=OSError("disk failure")):
        with pytest.raises(OSError):
            db.flush()
    with pytest.raises(RuntimeError):
        db.set("b", b"bad")
    db.close()  # Failed memory must never overwrite the disk state.
    recovered = engine(tmp_path)
    assert recovered.get("a") == b"v" and recovered.last_seq == 1


def test_snapshot_install_failure_reopens_from_new_snapshot(tmp_path):
    replica = engine(tmp_path, "replica")
    snapshot = SnapshotState("test", 9, True, {"a": b"v"})
    with patch.object(replica._wal, "clear", side_effect=OSError("disk failure")):
        with pytest.raises(OSError):
            replica.install_snapshot(snapshot.wire())
    with pytest.raises(RuntimeError):
        replica.get("a")
    reopened = engine(tmp_path, "replica")
    assert reopened.last_seq == 9 and reopened.get("a") == b"v"


def test_group_binding_and_standalone_rejection(tmp_path):
    db = engine(tmp_path)
    db.set("a", b"v")
    with pytest.raises(ValueError, match="another group"):
        engine(tmp_path, group="other")
    with pytest.raises(ValueError):
        KVEngine(FileStorage(tmp_path / "snapshot.json"), FileWAL(tmp_path / "wal.bin"))
    legacy = tmp_path / "legacy"
    standalone = KVEngine(FileStorage(legacy / "snapshot.json"), FileWAL(legacy / "wal.bin"))
    standalone.set("a", b"old")
    with pytest.raises(ValueError):
        engine(legacy)


def test_corrupt_and_gapped_wal_are_rejected(tmp_path):
    db = engine(tmp_path)
    db.set("a", b"v")
    db._wal.append(json.dumps({"seq": 3, "op": "delete", "key": "a"}).encode())
    with pytest.raises(ValueError, match="contiguous"):
        engine(tmp_path)
    path = tmp_path / "wal.bin"
    contents = bytearray(path.read_bytes())
    contents[5] ^= 1
    path.write_bytes(contents)
    with pytest.raises(ValueError, match="CRC32"):
        engine(tmp_path)


@pytest.mark.parametrize("raw", [
    {"seq": True, "op": "delete", "key": "a"},
    {"seq": 0, "op": "delete", "key": "a"},
    {"seq": 1, "op": "set", "key": "a", "value": "!"},
    {"seq": 1, "op": "unknown", "key": "a"},
])
def test_invalid_operation_rejected(raw):
    with pytest.raises(ValueError):
        Operation.parse(raw)


def test_directory_lock_released(tmp_path):
    lock = DirectoryLock(tmp_path)
    try:
        with pytest.raises(RuntimeError):
            DirectoryLock(tmp_path)
    finally:
        lock.close()
    DirectoryLock(tmp_path).close()


def test_http_roles_and_protocol(tmp_path):
    with TestClient(create_app(tmp_path, ReplicationConfig("leader", "test"))) as client:
        assert client.put("/kv/a", json={"value": "dg=="}).status_code == 200
        assert client.get("/internal/replication/changes?after=0").json()["operations"][0]["seq"] == 1
        assert client.get("/internal/replication/changes?after=-1").status_code == 422
        assert client.get("/internal/replication/changes?after=0&limit=1001").status_code == 422
        assert client.get("/internal/replication/changes?after=2").json()["code"] == "position_ahead"
        client.post("/flush")
        assert client.get("/internal/replication/changes?after=0").json()["code"] == "snapshot_required"
        assert client.get("/internal/replication/snapshot").json()["last_seq"] == 1
    with TestClient(create_app(tmp_path / "r", ReplicationConfig("replica", "test", "http://127.0.0.1:1", timeout=0.1))) as client:
        assert client.get("/kv/a").status_code == 503
        assert client.put("/kv/a", json={"value": "dg=="}).status_code == 403
        assert client.delete("/kv/a").status_code == 403
        assert client.get("/internal/replication/snapshot").status_code == 403
        assert client.get("/replication/status").json()["ready"] is False


def test_pause_observes_leader_without_applying_and_resumes(tmp_path):
    import httpx
    from kvdb.replication.worker import ReplicationWorker

    leader = engine(tmp_path / "l")
    replica = engine(tmp_path / "r", "replica")
    worker = ReplicationWorker(replica, ReplicationConfig("replica", "test", "http://leader"))
    def serve(request):
        if request.url.path.endswith("snapshot"):
            result = leader.export_snapshot()
        elif request.url.path.endswith("changes"):
            result = leader.changes(int(request.url.params["after"]), 1000)
        else:
            result = leader.status()
        return httpx.Response(200, json=result)
    with httpx.Client(base_url="http://leader", transport=httpx.MockTransport(serve)) as client:
        worker.tick(client)
        assert replica.initialized
        worker.set_paused(True)
        frozen = replica.last_seq
        leader.set("user-1", b"10")
        worker.tick(client)
        assert replica.last_seq == frozen
        assert worker.status()["paused"] and worker.status()["lag"] == 1
        assert replica.get("user-1") is None
        worker.set_paused(False)
        worker.tick(client)
        assert replica.get("user-1") == b"10"
        assert worker.status()["lag"] == 0


def test_pause_acknowledges_inflight_transfer(tmp_path):
    from threading import Event, Thread
    from kvdb.replication.worker import ReplicationWorker

    replica = engine(tmp_path, "replica")
    worker = ReplicationWorker(replica, ReplicationConfig("replica", "test", "http://leader"))
    entered, release, acknowledged = Event(), Event(), Event()
    def transfer():
        with worker.transfer_lock:
            entered.set()
            assert release.wait(3)
    def pause():
        worker.set_paused(True)
        acknowledged.set()
    transfer_thread = Thread(target=transfer)
    pause_thread = Thread(target=pause)
    transfer_thread.start()
    assert entered.wait(1)
    pause_thread.start()
    try:
        assert not acknowledged.wait(.05)
    finally:
        release.set()
        transfer_thread.join(3)
        pause_thread.join(3)
    assert acknowledged.is_set() and worker.status()["paused"]
