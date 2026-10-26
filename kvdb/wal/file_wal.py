"""Файловый WAL."""

from collections.abc import Iterator
import os
from pathlib import Path
import struct
import zlib

from kvdb._files import ensure_directory, sync_directory
from kvdb.interfaces.wal import WAL


class FileWAL(WAL):
    """Журнал: uint32 длина, payload, uint32 CRC32 (big-endian)."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._ready = False
        self._failed = False
        ensure_directory(self.path.parent)
        try:
            with self.path.open("xb") as stream:
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            pass
        # Синхронизируем и при повторном открытии: предыдущая попытка
        # создания могла завершиться между созданием файла и fsync каталога.
        sync_directory(self.path.parent)

    def _check(self) -> None:
        if self._failed:
            raise RuntimeError("WAL требует повторного открытия после ошибки записи")

    def append(self, record: bytes) -> None:
        self._check()
        if not isinstance(record, bytes):
            raise TypeError("Запись WAL должна иметь тип bytes")
        frame = struct.pack(">I", len(record)) + record + struct.pack(">I", zlib.crc32(record))
        if not self._ready:
            for _ in self.replay():
                pass
        try:
            with self.path.open("ab") as stream:
                if stream.write(frame) != len(frame):
                    raise OSError("Неполная запись WAL")
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            self._failed = True
            raise

    def replay(self) -> Iterator[bytes]:
        self._check()
        with self.path.open("r+b") as stream:
            valid_end = 0
            while True:
                header = stream.read(4)
                if not header:
                    break
                if len(header) != 4:
                    self._truncate_tail(stream, valid_end)
                    break
                length = struct.unpack(">I", header)[0]
                # Проверяем размер до чтения, чтобы повреждённая длина не
                # заставляла выделять гигабайты памяти.
                remaining = os.fstat(stream.fileno()).st_size - stream.tell()
                if remaining < length + 4:
                    self._truncate_tail(stream, valid_end)
                    break
                payload = stream.read(length)
                checksum = struct.unpack(">I", stream.read(4))[0]
                if zlib.crc32(payload) != checksum:
                    raise ValueError(f"Повреждён WAL {self.path}: CRC32 по смещению {valid_end}")
                valid_end = stream.tell()
                yield payload
        self._ready = True

    @staticmethod
    def _truncate_tail(stream, valid_end: int) -> None:
        stream.truncate(valid_end)
        stream.flush()
        os.fsync(stream.fileno())

    def clear(self) -> None:
        self._check()
        try:
            with self.path.open("r+b") as stream:
                stream.truncate(0)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            self._failed = True
            raise
        self._ready = True
