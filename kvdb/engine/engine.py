"""Связь состояния в памяти, снимка и WAL."""

import base64
import binascii
import json
from threading import RLock

from kvdb.interfaces import KeyValueStore, SnapshotStorage, WAL


class KVEngine(KeyValueStore):
    """База с последовательными записями и короткой блокировкой памяти."""

    def __init__(self, storage: SnapshotStorage, wal: WAL):
        self._storage = storage
        self._wal = wal
        # Если нужны обе блокировки, сначала захватываем _write_lock.
        self._write_lock = RLock()
        self._state_lock = RLock()
        self._closed = False
        self._failed = False
        self._data = storage.load()
        for record in wal.replay():
            self._apply_record(record)

    def _apply_record(self, record: bytes) -> None:
        try:
            operation = json.loads(record.decode("utf-8"))
            if not isinstance(operation, dict) or not isinstance(operation.get("key"), str):
                raise ValueError("неверная структура операции")
            key = operation["key"]
            if operation.get("op") == "set" and set(operation) == {"op", "key", "value"}:
                value = operation["value"]
                if not isinstance(value, str):
                    raise ValueError("значение должно быть строкой Base64")
                self._data[key] = base64.b64decode(value.encode("ascii"), validate=True)
            elif operation.get("op") == "delete" and set(operation) == {"op", "key"}:
                self._data.pop(key, None)
            else:
                raise ValueError("неизвестная или неверная операция")
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise ValueError(f"Повреждённая операция WAL: {exc}") from exc

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError("База закрыта")
        if self._failed:
            raise RuntimeError("База требует повторного открытия после ошибки WAL")

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str):
            raise TypeError("Ключ должен иметь тип str")

    def get(self, key: str) -> bytes | None:
        with self._state_lock:
            self._check()
            self._validate_key(key)
            return self._data.get(key)

    def _append(self, operation: dict) -> None:
        record = json.dumps(operation, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        self._wal.append(record)

    def _mark_failed(self) -> None:
        # Неизвестно, успела ли запись попасть на диск. Старый словарь
        # нельзя сохранять поверх журнала при последующем close().
        with self._state_lock:
            self._failed = True

    def set(self, key: str, value: bytes) -> None:
        with self._write_lock:
            with self._state_lock:
                self._check()
                self._validate_key(key)
                if not isinstance(value, bytes):
                    raise TypeError("Значение должно иметь тип bytes")
            try:
                self._append({"op": "set", "key": key, "value": base64.b64encode(value).decode("ascii")})
                with self._state_lock:
                    self._data[key] = value
            except BaseException:
                self._mark_failed()
                raise

    def delete(self, key: str) -> None:
        with self._write_lock:
            with self._state_lock:
                self._check()
                self._validate_key(key)
            try:
                self._append({"op": "delete", "key": key})
                with self._state_lock:
                    self._data.pop(key, None)
            except BaseException:
                self._mark_failed()
                raise

    def flush(self) -> None:
        with self._write_lock:
            with self._state_lock:
                self._check()
                snapshot = self._data.copy()
            self._storage.save(snapshot)
            try:
                self._wal.clear()
            except BaseException:
                self._mark_failed()
                raise

    def close(self) -> None:
        with self._write_lock:
            with self._state_lock:
                if self._closed:
                    return
                failed = self._failed
            # Ошибка WAL требует восстановления с диска, а не снимка старой памяти.
            if not failed:
                self.flush()
            with self._state_lock:
                self._closed = True

    def __enter__(self):
        with self._state_lock:
            self._check()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
