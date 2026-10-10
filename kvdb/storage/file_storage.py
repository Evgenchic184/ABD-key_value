"""Файловый SnapshotStorage."""

import base64
import binascii
import json
import os
from pathlib import Path
import tempfile
from collections.abc import Mapping

from kvdb._files import ensure_directory, sync_directory
from kvdb.interfaces.storage import SnapshotStorage


class FileStorage(SnapshotStorage):
    """Атомарный JSON-снимок со строковыми ключами и Base64-значениями."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)

    def load(self) -> dict[str, bytes]:
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return {}
        try:
            snapshot = json.loads(raw.decode("utf-8"))
            if not isinstance(snapshot, dict) or set(snapshot) != {"version", "data"}:
                raise ValueError("неверная структура снимка")
            if type(snapshot["version"]) is not int or snapshot["version"] != 1:
                raise ValueError("неизвестная версия снимка")
            if not isinstance(snapshot["data"], dict):
                raise ValueError("data должен быть словарём")
            result = {}
            for key, value in snapshot["data"].items():
                if not isinstance(value, str):
                    raise ValueError("значение должно быть строкой Base64")
                result[key] = base64.b64decode(value.encode("ascii"), validate=True)
            return result
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise ValueError(f"Повреждённый снимок {self.path}: {exc}") from exc

    def save(self, data: Mapping[str, bytes]) -> None:
        encoded = {}
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, bytes):
                raise TypeError("Снимок требует строковые ключи и bytes-значения")
            encoded[key] = base64.b64encode(value).decode("ascii")
        payload = json.dumps({"version": 1, "data": encoded}, ensure_ascii=True)
        ensure_directory(self.path.parent)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.path.parent,
                prefix=f".{self.path.name}.", suffix=".tmp", delete=False,
            ) as stream:
                temporary = Path(stream.name)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            sync_directory(self.path.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
