"""Общие операции для устойчивого файлового хранения (POSIX)."""

import os
from pathlib import Path


def sync_directory(path: Path) -> None:
    """Закрепить создание или замену файлов в каталоге."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_directory(path: Path) -> None:
    """Создать каталог и закрепить его запись и записи всех предков."""
    path = Path(os.path.abspath(path))
    if path == path.parent:
        return
    ensure_directory(path.parent)
    path.mkdir(exist_ok=True)
    # Существующий каталог мог остаться после сбоя между mkdir и fsync.
    # Повторное открытие должно закрепить и такую запись в родителе.
    sync_directory(path.parent)
