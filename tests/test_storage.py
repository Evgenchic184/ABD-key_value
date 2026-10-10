"""Проверки дисковых снимков."""

import json
from unittest.mock import patch

import pytest

from kvdb import FileStorage


def test_missing_and_empty_snapshot(storage):
    assert storage.load() == {}
    storage.save({})
    assert storage.load() == {}


def test_roundtrip_and_overwrite(snapshot_path, storage):
    data = {"ключ": bytes(range(256)), "": b"", "text": b"hello"}
    storage.save(data)
    assert FileStorage(snapshot_path).load() == data
    storage.save({"new": b"value"})
    assert storage.load() == {"new": b"value"}


def test_nested_directory(snapshot_path):
    storage = FileStorage(snapshot_path.parent / "nested" / "more" / "snapshot.json")
    storage.save({"a": b"b"})
    assert storage.load() == {"a": b"b"}


@pytest.mark.parametrize(
    "raw",
    [b"", b"\xff", b"{", b"[]", b"null"]
    + [
        json.dumps(value).encode()
        for value in (
            {"version": 2, "data": {}},
            {"version": True, "data": {}},
            {"version": 1, "data": []},
            {"version": 1, "data": {"x": 123}},
            {"version": 1, "data": {"x": "%%%"}},
            {"version": 1, "data": {"x": "я"}},
        )
    ],
)
def test_corrupt_formats(snapshot_path, storage, raw):
    snapshot_path.write_bytes(raw)
    with pytest.raises(ValueError, match="снимок"):
        storage.load()


@pytest.mark.parametrize(
    "target",
    ("kvdb.storage.file_storage.os.fsync", "kvdb.storage.file_storage.os.replace"),
)
def test_failures_before_replace_preserve_previous_snapshot(
    snapshot_path, storage, target
):
    storage.save({"old": b"data"})
    original = snapshot_path.read_bytes()
    with patch(target, side_effect=OSError("disk failure")):
        with pytest.raises(OSError):
            storage.save({"new": b"data"})
    assert snapshot_path.read_bytes() == original
    assert list(snapshot_path.parent.glob("*.tmp")) == []


def test_directory_sync_failure_is_reported(storage):
    storage.save({"old": b"data"})
    with patch(
        "kvdb.storage.file_storage.sync_directory", side_effect=OSError("sync failure")
    ):
        with pytest.raises(OSError):
            storage.save({"new": b"data"})
    assert storage.load() == {"new": b"data"}


@pytest.mark.parametrize("data", ({1: b"v"}, {"x": "text"}))
def test_invalid_types_do_not_replace_snapshot(storage, data):
    storage.save({"old": b"data"})
    with pytest.raises(TypeError):
        storage.save(data)
    assert storage.load() == {"old": b"data"}
