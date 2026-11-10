"""Проверенная конфигурация роутера и набора шардов."""

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
from urllib.parse import urlparse


def _origin(value, field_name: str) -> str:
    """Проверить HTTP(S)-origin и убрать завершающий слэш."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty HTTP(S) origin")
    parsed = urlparse(value)
    if (parsed.scheme not in ("http", "https") or not parsed.netloc
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError(f"{field_name} must be an HTTP(S) origin")
    try:
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as exc:
        raise ValueError(f"{field_name} has an invalid host or port") from exc
    if not hostname or any(char.isspace() for char in hostname):
        raise ValueError(f"{field_name} must include a valid host")
    if parsed.netloc.endswith(":"):
        raise ValueError(f"{field_name} has an invalid port")
    return value.rstrip("/")


def _identifier(value, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without edge spaces")
    return value


@dataclass(frozen=True)
class ShardConfig:
    """Один шард: ведущий и необязательные реплики."""

    id: str
    leader: str
    replicas: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, "id", _identifier(self.id, "shard id"))
        object.__setattr__(self, "leader", _origin(self.leader, f"shard {self.id} leader"))
        if not isinstance(self.replicas, (list, tuple)):
            raise ValueError(f"shard {self.id} replicas must be a list")
        replicas = tuple(_origin(url, f"shard {self.id} replica") for url in self.replicas)
        if len(set(replicas)) != len(replicas):
            raise ValueError(f"shard {self.id} has duplicate replicas")
        if self.leader in replicas:
            raise ValueError(f"shard {self.id} leader is listed as a replica")
        object.__setattr__(self, "replicas", replicas)

    @property
    def read_urls(self) -> tuple[str, ...]:
        """Реплики для распределения чтений; без реплик — ведущий."""
        return self.replicas or (self.leader,)

    def wire(self) -> dict:
        return {"id": self.id, "leader": self.leader, "replicas": list(self.replicas)}


@dataclass(frozen=True)
class ShardingConfig:
    """Топология шардов и параметры маршрутизации."""

    shards: tuple[ShardConfig, ...]
    virtual_nodes: int = 128
    timeout: float = 2.0
    reads: str = "replica"

    def __post_init__(self):
        if isinstance(self.shards, (str, bytes)) or not isinstance(self.shards, (list, tuple)):
            raise ValueError("shards must be a list")
        shards = tuple(self.shards)
        if not shards:
            raise ValueError("at least one shard is required")
        if not all(isinstance(shard, ShardConfig) for shard in shards):
            raise ValueError("shards must contain ShardConfig objects")
        if len({shard.id for shard in shards}) != len(shards):
            raise ValueError("shard ids must be unique")
        urls = [url for shard in shards for url in (shard.leader, *shard.replicas)]
        if len(set(urls)) != len(urls):
            raise ValueError("node URLs must be unique across shards")
        if type(self.virtual_nodes) is not int or not 1 <= self.virtual_nodes <= 65536:
            raise ValueError("virtual_nodes must be an integer between 1 and 65536")
        if (not isinstance(self.timeout, (int, float)) or isinstance(self.timeout, bool)
                or not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        if self.reads not in ("replica", "leader"):
            raise ValueError("reads must be 'replica' or 'leader'")
        object.__setattr__(self, "shards", shards)

    @classmethod
    def from_mapping(cls, raw) -> "ShardingConfig":
        if not isinstance(raw, Mapping):
            raise ValueError("sharding config must be an object")
        allowed = {"shards", "virtual_nodes", "timeout", "reads"}
        if set(raw) - allowed:
            raise ValueError(f"unknown sharding config fields: {sorted(set(raw) - allowed)}")
        values = raw.get("shards")
        if not isinstance(values, list):
            raise ValueError("shards must be a list")
        shards = []
        for item in values:
            if not isinstance(item, Mapping) or set(item) - {"id", "leader", "replicas"}:
                raise ValueError("each shard must contain id, leader and optional replicas")
            if "id" not in item or "leader" not in item:
                raise ValueError("each shard requires id and leader")
            replicas = item.get("replicas", [])
            if not isinstance(replicas, list):
                raise ValueError("shard replicas must be a list")
            shards.append(ShardConfig(item["id"], item["leader"], tuple(replicas)))
        return cls(tuple(shards), raw.get("virtual_nodes", 128),
                   raw.get("timeout", 2.0), raw.get("reads", "replica"))

    @classmethod
    def from_file(cls, path: str | Path) -> "ShardingConfig":
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid sharding config JSON: {exc}") from exc
        return cls.from_mapping(raw)

    def wire(self) -> dict:
        return {"virtual_nodes": self.virtual_nodes, "timeout": self.timeout,
                "reads": self.reads, "shards": [shard.wire() for shard in self.shards]}
