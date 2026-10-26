"""Проверки CLI через subprocess и обработчиков завершения."""

import io
from unittest.mock import patch

from kvdb import FileStorage, FileWAL, KVEngine
from kvdb.cli.main import main


def test_commands_and_restart(run_cli):
    result = run_cli(
        'set "my key" "hello world"\nget "my key"\nset empty ""\nget empty\nflush\nexit\n'
    )
    assert result.returncode == 0, result.stderr
    assert "b'hello world'" in result.stdout
    assert "b''" in result.stdout
    result = run_cli('get "my key"\ndelete "my key"\nget "my key"\nexit\n')
    assert result.returncode == 0, result.stderr
    assert "b'hello world'" in result.stdout
    assert "Ключ не найден" in result.stdout
    result = run_cli('get "my key"\nexit\n')
    assert result.returncode == 0, result.stderr
    assert "Ключ не найден" in result.stdout


def test_input_errors_are_recoverable_and_eof_saves(run_cli, tmp_path):
    result = run_cli(
        'unknown\nset onlykey\nset "unterminated\nhelp\n\nset ключ значение\n'
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr.count("Ошибка ввода") == 3
    assert "Команды:" in result.stdout
    assert FileStorage(tmp_path / "snapshot.json").load() == {
        "ключ": "значение".encode("utf-8")
    }
    assert (tmp_path / "wal.bin").read_bytes() == b""


def test_corrupt_snapshot_exits_nonzero(run_cli, tmp_path):
    (tmp_path / "snapshot.json").write_text("broken")
    result = run_cli("get a\n")
    assert result.returncode != 0
    assert "Ошибка хранения" in result.stderr


def test_corrupt_wal_exits_nonzero(run_cli, wal):
    wal.append(b"invalid operation")
    result = run_cli("get a\n")
    assert result.returncode != 0
    assert "Ошибка хранения" in result.stderr


def test_keyboard_interrupt_closes_database(tmp_path):
    with (
        patch("builtins.input", side_effect=["set a value", KeyboardInterrupt]),
        patch("sys.stdout", new_callable=io.StringIO),
        patch("sys.stderr", new_callable=io.StringIO),
    ):
        assert main(["--data-dir", str(tmp_path)]) == 130
    assert FileStorage(tmp_path / "snapshot.json").load() == {"a": b"value"}


def test_storage_error_exits_nonzero_and_preserves_uncertain_wal(tmp_path):
    original_append = FileWAL.append

    def uncertain(wal, record):
        original_append(wal, record)
        raise OSError("write failure")

    with (
        patch("builtins.input", return_value="set a value"),
        patch.object(FileWAL, "append", new=uncertain),
        patch("sys.stderr", new_callable=io.StringIO) as errors,
    ):
        assert main(["--data-dir", str(tmp_path)]) == 1
        assert "Ошибка хранения" in errors.getvalue()
    with KVEngine(
        FileStorage(tmp_path / "snapshot.json"), FileWAL(tmp_path / "wal.bin")
    ) as db:
        assert db.get("a") == b"value"


def test_close_failure_exits_nonzero(tmp_path):
    with (
        patch("builtins.input", side_effect=["set a value", "exit"]),
        patch.object(FileStorage, "save", side_effect=OSError("save failure")),
        patch("sys.stdout", new_callable=io.StringIO),
        patch("sys.stderr", new_callable=io.StringIO) as errors,
    ):
        assert main(["--data-dir", str(tmp_path)]) == 1
        assert "Ошибка закрытия" in errors.getvalue()
