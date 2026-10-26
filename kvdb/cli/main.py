"""Точка входа CLI."""

import argparse
from pathlib import Path
import shlex
import sys

from kvdb import FileStorage, FileWAL, KVEngine


HELP = """Команды:
  set KEY VALUE   записать UTF-8 значение (кавычки поддерживаются)
  get KEY         показать bytes-значение
  delete KEY      удалить ключ
  flush           сохранить снимок и очистить WAL
  exit            сохранить и выйти
  help            показать справку"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Локальная key-value база")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    db = None
    status = 0
    try:
        db = KVEngine(FileStorage(args.data_dir / "snapshot.json"), FileWAL(args.data_dir / "wal.bin"))
        if sys.stdin.isatty():
            print(HELP)
        while True:
            try:
                line = input("kvdb> " if sys.stdin.isatty() else "")
            except EOFError:
                break
            try:
                parts = shlex.split(line)
            except ValueError as exc:
                print(f"Ошибка ввода: {exc}", file=sys.stderr)
                continue
            if not parts:
                continue
            command, *arguments = parts
            arity = {"set": 2, "get": 1, "delete": 1, "flush": 0, "exit": 0, "help": 0}
            if command not in arity or len(arguments) != arity[command]:
                print("Ошибка ввода: неизвестная команда или число аргументов; см. help", file=sys.stderr)
                continue
            if command == "exit":
                break
            if command == "help":
                print(HELP)
            elif command == "get":
                value = db.get(arguments[0])
                print("Ключ не найден" if value is None else repr(value))
            elif command == "set":
                db.set(arguments[0], arguments[1].encode("utf-8"))
                print("OK")
            elif command == "delete":
                db.delete(arguments[0])
                print("OK")
            elif command == "flush":
                db.flush()
                print("OK")
    except KeyboardInterrupt:
        print("\nЗавершение работы", file=sys.stderr)
        status = 130
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Ошибка хранения: {exc}", file=sys.stderr)
        status = 1
    finally:
        if db is not None:
            try:
                db.close()
            except (OSError, ValueError, RuntimeError) as exc:
                print(f"Ошибка закрытия: {exc}", file=sys.stderr)
                status = 1
    return status


if __name__ == "__main__":
    sys.exit(main())
