"""Validated wire and disk representations for replication."""

import base64
from dataclasses import dataclass
from typing import Literal


def integer(value, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def decode(value: str) -> bytes:
    if not isinstance(value, str):
        raise ValueError("value must be Base64 text")
    return base64.b64decode(value.encode("ascii"), validate=True)


@dataclass(frozen=True)
class Operation:
    seq: int
    op: Literal["set", "delete"]
    key: str
    value: bytes | None = None
    epoch: int = 0

    @classmethod
    def parse(cls, raw: dict) -> "Operation":
        if not isinstance(raw, dict):
            raise ValueError("operation must be an object")
        seq = integer(raw.get("seq"), "seq")
        op, key = raw.get("op"), raw.get("key")
        expected = {"seq", "op", "key", "value"} if op == "set" else {"seq", "op", "key"}
        if seq == 0 or op not in ("set", "delete") or not isinstance(key, str) or set(raw) - {"epoch"} != expected:
            raise ValueError("invalid operation")
        return cls(seq, op, key, decode(raw["value"]) if op == "set" else None,
                   integer(raw.get("epoch", 0), "epoch"))

    def wire(self) -> dict:
        raw = {"seq": self.seq, "op": self.op, "key": self.key}
        if self.op == "set":
            raw["value"] = base64.b64encode(self.value).decode("ascii")
        if self.epoch:
            raw["epoch"] = self.epoch
        return raw


@dataclass(frozen=True)
class SnapshotState:
    group_id: str
    last_seq: int
    initialized: bool
    data: dict[str, bytes]
    epoch: int = 0

    @classmethod
    def parse(cls, raw: dict) -> "SnapshotState":
        fields = {"version", "group_id", "last_seq", "initialized", "data"}
        if not isinstance(raw, dict) or set(raw) - {"epoch"} != fields:
            raise ValueError("invalid cluster snapshot")
        if type(raw["version"]) is not int or raw["version"] != 2:
            raise ValueError("cluster snapshot version must be 2")
        if not isinstance(raw["group_id"], str) or not raw["group_id"]:
            raise ValueError("invalid group_id")
        seq = integer(raw["last_seq"], "last_seq")
        if type(raw["initialized"]) is not bool or not isinstance(raw["data"], dict):
            raise ValueError("invalid snapshot state")
        if not all(isinstance(key, str) for key in raw["data"]):
            raise ValueError("invalid snapshot keys")
        data = {key: decode(value) for key, value in raw["data"].items()}
        if not raw["initialized"] and (seq or data):
            raise ValueError("uninitialized snapshot must be empty")
        return cls(raw["group_id"], seq, raw["initialized"], data, integer(raw.get("epoch", 0), "epoch"))

    def wire(self) -> dict:
        raw = {"version": 2, "group_id": self.group_id, "last_seq": self.last_seq,
                "initialized": self.initialized,
                "data": {key: base64.b64encode(value).decode("ascii") for key, value in self.data.items()}}

        if self.epoch:
            raw["epoch"] = self.epoch
        return raw


class ReadOnlyError(PermissionError):
    """Public mutations on a replica are forbidden."""


class ReplicationConflict(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)
