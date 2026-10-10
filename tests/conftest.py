"""Общие фикстуры: отдельный каталог и экземпляры хранилищ для каждого теста."""

import subprocess
import sys
from pathlib import Path

import pytest

from kvdb import FileStorage, FileWAL, KVEngine

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def snapshot_path(tmp_path):
    return tmp_path / "snapshot.json"


@pytest.fixture
def wal_path(tmp_path):
    return tmp_path / "wal.bin"


@pytest.fixture
def storage(snapshot_path):
    return FileStorage(snapshot_path)


@pytest.fixture
def wal(wal_path):
    return FileWAL(wal_path)


@pytest.fixture
def db(storage, wal):
    # Не вызываем close автоматически: проверки восстановления намеренно
    # оставляют WAL без снимка. Открытых файлов движок между вызовами не держит.
    return KVEngine(storage, wal)


@pytest.fixture
def reopen(snapshot_path, wal_path):
    def open_database():
        return KVEngine(FileStorage(snapshot_path), FileWAL(wal_path))

    return open_database


@pytest.fixture
def run_cli(tmp_path):
    def run(commands):
        return subprocess.run(
            [sys.executable, "-m", "kvdb.cli.main", "--data-dir", str(tmp_path)],
            cwd=ROOT,
            input=commands,
            capture_output=True,
            text=True,
            timeout=10,
        )

    return run
