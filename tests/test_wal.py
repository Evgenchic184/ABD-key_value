"""Проверки журнала изменений."""

import os
import struct
import zlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kvdb import FileWAL
from kvdb._files import sync_directory


def test_order_empty_payload_and_wire_format(wal, wal_path):
    assert list(wal.replay()) == []
    records = [b"first", b"", bytes(range(256))]
    for record in records:
        wal.append(record)
    expected = b"".join(
        (
            struct.pack(">I", len(r)) + r + struct.pack(">I", zlib.crc32(r))
            for r in records
        )
    )
    assert wal_path.read_bytes() == expected
    assert list(FileWAL(wal_path).replay()) == records


def test_clear_and_reuse(wal, wal_path):
    wal.append(b"old")
    wal.clear()
    wal.clear()
    assert wal_path.read_bytes() == b""
    wal.append(b"new")
    assert list(wal.replay()) == [b"new"]


@pytest.mark.parametrize(
    "tail",
    [
        b"\x00",
        b"\x00\x00\x00",
        struct.pack(">I", 5),
        struct.pack(">I", 5) + b"abc",
        struct.pack(">I", 3) + b"abc" + b"\x00\x00",
        struct.pack(">I", 4294967295),
    ],
)
def test_incomplete_tail_is_trimmed_before_append(wal, wal_path, tail):
    wal.append(b"saved")
    original = wal_path.read_bytes()
    wal_path.write_bytes(original + tail)
    reopened = FileWAL(wal_path)
    assert list(reopened.replay()) == [b"saved"]
    assert wal_path.read_bytes() == original
    reopened.append(b"next")
    assert list(reopened.replay()) == [b"saved", b"next"]


def test_append_without_explicit_replay_repairs_tail(wal, wal_path):
    wal.append(b"saved")
    with wal_path.open("ab") as stream:
        stream.write(b"\x00")
    reopened = FileWAL(wal_path)
    reopened.append(b"next")
    assert list(reopened.replay()) == [b"saved", b"next"]


def test_checksum_corruption_fails_without_truncation(wal, wal_path):
    wal.append(b"saved")
    raw = bytearray(wal_path.read_bytes())
    raw[-1] ^= 1
    wal_path.write_bytes(raw)
    with pytest.raises(ValueError, match="CRC32"):
        list(FileWAL(wal_path).replay())
    assert wal_path.read_bytes() == raw


def test_append_waits_for_fsync(wal, wal_path):
    list(wal.replay())
    with patch(
        "kvdb.wal.file_wal.os.fsync", side_effect=OSError("sync failure")
    ) as sync:
        with pytest.raises(OSError):
            wal.append(b"uncertain")
        sync.assert_called_once()
    with pytest.raises(RuntimeError):
        wal.append(b"later")
    assert list(FileWAL(wal_path).replay()) == [b"uncertain"]


@pytest.mark.parametrize("short_write", (False, True))
def test_write_error_and_short_write_block_further_append(wal_path, short_write):
    wal = FileWAL(wal_path)
    list(wal.replay())
    stream = MagicMock()
    stream.__enter__.return_value = stream
    if short_write:
        stream.write.return_value = 1
    else:
        stream.write.side_effect = OSError("write failure")
    with patch.object(Path, "open", return_value=stream):
        with pytest.raises(OSError):
            wal.append(b"value")
    with pytest.raises(RuntimeError):
        wal.append(b"later")


def test_creation_syncs_file_and_directory(wal_path):
    path = wal_path.parent / "new.bin"
    with (
        patch("kvdb._files.sync_directory"),
        patch("kvdb.wal.file_wal.os.fsync", wraps=os.fsync) as sync,
    ):
        FileWAL(path)
    assert sync.call_count == 2


def test_reopen_syncs_ancestor_after_directory_creation_failure(wal_path):
    path = wal_path.parent / "nested" / "more" / "wal.bin"
    parent = path.parent.parent

    def fail_new_directory_sync(directory):
        if directory == parent:
            raise OSError("directory sync failure")
        sync_directory(directory)

    with patch("kvdb._files.sync_directory", side_effect=fail_new_directory_sync):
        with pytest.raises(OSError):
            FileWAL(path)
    assert path.parent.is_dir()
    assert not path.exists()
    with patch("kvdb._files.sync_directory", wraps=sync_directory) as sync:
        reopened = FileWAL(path)
    synced = [call.args[0] for call in sync.call_args_list]
    assert parent in synced
    assert parent.parent in synced
    reopened.append(b"durable")
    assert list(FileWAL(path).replay()) == [b"durable"]


def test_reopen_retries_directory_sync_after_creation_failure(wal_path):
    path = wal_path.parent / "new.bin"
    with patch(
        "kvdb.wal.file_wal.sync_directory",
        side_effect=OSError("directory sync failure"),
    ):
        with pytest.raises(OSError):
            FileWAL(path)
    assert path.exists()
    with patch("kvdb.wal.file_wal.sync_directory") as sync:
        reopened = FileWAL(path)
        sync.assert_called_once_with(path.parent)
    reopened.append(b"durable")
    assert list(reopened.replay()) == [b"durable"]


def test_clear_sync_failure_blocks_wal(wal):
    wal.append(b"value")
    with patch("kvdb.wal.file_wal.os.fsync", side_effect=OSError("sync failure")):
        with pytest.raises(OSError):
            wal.clear()
    with pytest.raises(RuntimeError):
        wal.append(b"later")
