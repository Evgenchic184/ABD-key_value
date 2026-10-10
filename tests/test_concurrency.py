"""Проверки порядка операций и доступности чтения при работе с диском."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from threading import Barrier, Event
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def initial_value(db):
    db.set("key", b"old")


@contextmanager
def pause(target, method, *, after=False, error=None):
    entered, release = (Event(), Event())
    original = getattr(target, method)

    def paused(*args):
        if after:
            original(*args)
        entered.set()
        if not release.wait(5):
            raise TimeoutError("test did not release operation")
        if error is not None:
            raise error
        if not after:
            return original(*args)

    with patch.object(target, method, side_effect=paused):
        try:
            yield entered
        finally:
            release.set()


def test_read_does_not_wait_for_wal_append(db, wal):
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause(wal, "append") as entered:
            writer = pool.submit(db.set, "key", b"new")
            assert entered.wait(2)
            assert pool.submit(db.get, "key").result(2) == b"old"
        writer.result(2)
    assert db.get("key") == b"new"


def test_writers_keep_wal_and_memory_in_same_order(db, reopen, wal):
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause(wal, "append", after=True) as entered:
            first = pool.submit(db.set, "key", b"first")
            assert entered.wait(2)
            second = pool.submit(db.delete, "key")
            with pytest.raises(TimeoutError):
                second.result(0.1)
        first.result(2)
        second.result(2)
    assert db.get("key") is None
    assert reopen().get("key") is None


def test_flush_waits_for_pending_write(db, reopen, storage, wal):
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause(wal, "append", after=True) as entered:
            writer = pool.submit(db.set, "key", b"new")
            assert entered.wait(2)
            flush = pool.submit(db.flush)
            with pytest.raises(TimeoutError):
                flush.result(0.1)
        writer.result(2)
        flush.result(2)
    assert storage.load() == {"key": b"new"}
    assert wal.path.read_bytes() == b""
    assert reopen().get("key") == b"new"


def test_read_during_snapshot_and_write_after_flush(db, reopen, storage):
    with ThreadPoolExecutor(max_workers=3) as pool:
        with pause(storage, "save") as entered:
            flush = pool.submit(db.flush)
            assert entered.wait(2)
            assert pool.submit(db.get, "key").result(2) == b"old"
            writer = pool.submit(db.set, "key", b"new")
            with pytest.raises(TimeoutError):
                writer.result(0.1)
        flush.result(2)
        writer.result(2)
    assert storage.load() == {"key": b"old"}
    assert db.get("key") == b"new"
    assert reopen().get("key") == b"new"


def test_close_blocks_queued_write(db, reopen, storage):
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause(storage, "save") as entered:
            close = pool.submit(db.close)
            assert entered.wait(2)
            writer = pool.submit(db.set, "key", b"new")
            with pytest.raises(TimeoutError):
                writer.result(0.1)
        close.result(2)
        with pytest.raises(RuntimeError, match="закрыта"):
            writer.result(2)
    assert reopen().get("key") == b"old"


def test_failed_write_blocks_queued_write_and_preserves_wal(db, reopen, wal):
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pause(
            wal, "append", after=True, error=OSError("uncertain write")
        ) as entered:
            first = pool.submit(db.set, "key", b"uncertain")
            assert entered.wait(2)
            second = pool.submit(db.set, "key", b"later")
            with pytest.raises(TimeoutError):
                second.result(0.1)
        with pytest.raises(OSError):
            first.result(2)
        with pytest.raises(RuntimeError):
            second.result(2)
    with pytest.raises(RuntimeError):
        db.get("key")
    db.close()
    assert reopen().get("key") == b"uncertain"


def test_concurrent_crud_and_flush_stress(db, reopen):
    writers = 4
    rounds = 25
    start = Barrier(writers + 2)
    stop = Event()
    values = {
        f"{writer}:{index}".encode()
        for writer in range(writers)
        for index in range(rounds)
    }
    db.set("shared", b"initial")

    def write(writer):
        start.wait(timeout=5)
        for index in range(rounds):
            key = f"{writer}/{index}"
            value = f"{writer}:{index}".encode()
            db.set(key, b"pending")
            db.set(key, value)
            if index % 5 == 0:
                db.delete(key)
            db.set("shared", value)

    def read():
        start.wait(timeout=5)
        reads = 0
        while True:
            assert db.get("shared") in values | {b"initial"}
            reads += 1
            if stop.wait(0.001):
                return reads

    def flush():
        start.wait(timeout=5)
        for _ in range(10):
            db.flush()
            stop.wait(0.001)

    with ThreadPoolExecutor(max_workers=writers + 2) as pool:
        writing = [pool.submit(write, writer) for writer in range(writers)]
        reading = pool.submit(read)
        flushing = pool.submit(flush)
        try:
            for future in writing:
                future.result(timeout=10)
            flushing.result(timeout=10)
        finally:
            stop.set()
        assert reading.result(timeout=5) > 0

    # Открываем после остановки потоков, без финального flush/close:
    # восстановление должно учитывать и снимок, и оставшийся журнал.
    recovered = reopen()
    for writer in range(writers):
        for index in range(rounds):
            key = f"{writer}/{index}"
            expected = None if index % 5 == 0 else f"{writer}:{index}".encode()
            assert db.get(key) == expected
            assert recovered.get(key) == expected
    assert recovered.get("shared") == db.get("shared")
    assert recovered.get("key") == b"old"
