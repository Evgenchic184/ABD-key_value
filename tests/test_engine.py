"""Проверки операций движка и восстановления состояния."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_crud(db):
    assert db.get("missing") is None
    db.delete("missing")
    db.set("ключ", bytes(range(256)))
    assert db.get("ключ") == bytes(range(256))
    db.set("ключ", b"new")
    assert db.get("ключ") == b"new"
    db.set("", b"")
    assert db.get("") == b""
    db.delete("ключ")
    assert db.get("ключ") is None


def test_snapshot_and_wal_recovery(db, reopen):
    db.set("keep", b"snapshot")
    db.set("delete", b"old")
    db.flush()
    db.set("keep", b"updated")
    db.set("new", b"wal")
    db.delete("delete")
    reopened = reopen()
    assert reopened.get("keep") == b"updated"
    assert reopened.get("new") == b"wal"
    assert reopened.get("delete") is None


def test_repeated_flush_and_close(db, reopen, wal):
    db.set("a", b"v")
    db.flush()
    db.flush()
    assert list(wal.replay()) == []
    db.close()
    db.close()
    for operation in (
        lambda: db.get("a"),
        lambda: db.set("a", b"b"),
        lambda: db.delete("a"),
        db.flush,
        db.__enter__,
    ):
        with pytest.raises(RuntimeError):
            operation()
    assert reopen().get("a") == b"v"


def test_context_manager_saves_on_exception(db, reopen):
    with pytest.raises(ValueError, match="application"):
        with db as db:
            db.set("a", b"v")
            raise ValueError("application")
    assert reopen().get("a") == b"v"


@pytest.mark.parametrize(
    "method, arguments",
    [
        pytest.param("get", (1,), id="get-key-type"),
        pytest.param("set", (1, b"v"), id="set-key-type"),
        pytest.param("set", ("a", "text"), id="set-value-type"),
        pytest.param("delete", (1,), id="delete-key-type"),
    ],
)
def test_invalid_inputs_do_not_poison_database(db, method, arguments):
    with pytest.raises(TypeError):
        getattr(db, method)(*arguments)
    db.set("valid", b"v")
    assert db.get("valid") == b"v"


def test_snapshot_failure_preserves_wal_and_allows_retry(db, reopen, storage, wal):
    db.set("a", b"v")
    original = wal.path.read_bytes()
    with patch.object(storage, "save", side_effect=OSError("snapshot failure")):
        with pytest.raises(OSError):
            db.flush()
    assert wal.path.read_bytes() == original
    assert reopen().get("a") == b"v"
    db.flush()
    assert wal.path.read_bytes() == b""


def test_snapshot_directory_sync_failure_does_not_clear_wal(db, reopen, wal):
    db.set("a", b"v")
    original = wal.path.read_bytes()
    with patch(
        "kvdb.storage.file_storage.sync_directory", side_effect=OSError("sync failure")
    ):
        with pytest.raises(OSError):
            db.flush()
    assert wal.path.read_bytes() == original
    assert reopen().get("a") == b"v"


def test_clear_failure_leaves_replay_idempotent(db, reopen, wal):
    db.set("a", b"old")
    db.flush()
    db.set("a", b"new")
    db.delete("a")
    db.set("b", b"kept")
    with patch.object(wal, "clear", side_effect=OSError("clear failure")):
        with pytest.raises(OSError):
            db.flush()
    with pytest.raises(RuntimeError):
        db.set("c", b"later")
    db.close()
    reopened = reopen()
    assert reopened.get("a") is None
    assert reopened.get("b") == b"kept"


def test_partial_clear_failure_recovers_from_snapshot(db, reopen, wal):
    db.set("a", b"v")
    original_clear = wal.clear

    def failed_clear():
        original_clear()
        raise OSError("failure after truncation")

    with patch.object(wal, "clear", side_effect=failed_clear):
        with pytest.raises(OSError):
            db.flush()
    assert reopen().get("a") == b"v"


def test_wal_write_failure_leaves_memory_unchanged_and_blocks_database(
    db, reopen, storage, wal
):
    db.set("a", b"old")
    with patch.object(wal, "append", side_effect=OSError("write failure")):
        with pytest.raises(OSError):
            db.set("a", b"new")
    assert db._data == {"a": b"old"}
    for operation in (
        lambda: db.get("a"),
        lambda: db.set("b", b"v"),
        lambda: db.delete("a"),
        db.flush,
    ):
        with pytest.raises(RuntimeError):
            operation()
    with patch.object(storage, "save") as save:
        db.close()
        save.assert_not_called()
    assert reopen().get("a") == b"old"


def test_failed_delete_preserves_memory(db, wal):
    db.set("a", b"old")
    with patch.object(wal, "append", side_effect=OSError("write failure")):
        with pytest.raises(OSError):
            db.delete("a")
    assert db._data == {"a": b"old"}


def test_uncertain_append_is_recovered_without_stale_snapshot(db, reopen, wal):
    db.set("a", b"old")
    original_append = wal.append

    def uncertain(record):
        original_append(record)
        raise OSError("failure after durable write")

    with patch.object(wal, "append", side_effect=uncertain):
        with pytest.raises(OSError):
            db.set("a", b"new")
    db.close()
    assert reopen().get("a") == b"new"


def test_interrupted_append_preserves_journal_for_recovery(db, reopen, wal):
    original_append = wal.append

    def interrupted(record):
        original_append(record)
        raise KeyboardInterrupt

    with patch.object(wal, "append", side_effect=interrupted):
        with pytest.raises(KeyboardInterrupt):
            db.set("a", b"v")
    db.close()
    assert reopen().get("a") == b"v"


@pytest.mark.parametrize(
    "record",
    [b"not json", b"[]", b"null"]
    + [
        json.dumps(value).encode()
        for value in (
            {"op": "unknown", "key": "a"},
            {"op": "set", "key": "a"},
            {"op": "set", "key": "a", "value": "%%%"},
            {"op": "set", "key": "a", "value": 1},
            {"op": "set", "key": "a", "value": "я"},
            {"op": "delete", "key": 1},
            {"op": "delete", "key": "a", "extra": 1},
        )
    ],
)
def test_invalid_wal_operations_fail_open(reopen, wal, record):
    wal.clear()
    wal.append(record)
    with pytest.raises(ValueError, match="операция WAL"):
        reopen()


def test_crash_recovery_in_separate_process(tmp_path):
    script = """
import os
from pathlib import Path
import sys
from kvdb import FileStorage, FileWAL, KVEngine
path = Path(sys.argv[1])
db = KVEngine(FileStorage(path / 'snapshot.json'), FileWAL(path / 'wal.bin'))
db.set('deleted', b'old')
db.set('updated', b'old')
db.flush()
db.delete('deleted')
db.set('updated', b'new')
db.set('binary', bytes(range(256)))
os._exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    check = """
from pathlib import Path
import sys
from kvdb import FileStorage, FileWAL, KVEngine
path = Path(sys.argv[1])
with KVEngine(FileStorage(path / 'snapshot.json'), FileWAL(path / 'wal.bin')) as db:
    assert db.get('deleted') is None
    assert db.get('updated') == b'new'
    assert db.get('binary') == bytes(range(256))
"""
    result = subprocess.run(
        [sys.executable, "-c", check, str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
