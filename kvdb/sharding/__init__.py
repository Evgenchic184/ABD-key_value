"""Прокси-шардирование поверх независимых групп репликации."""

from kvdb.sharding.config import ShardConfig, ShardingConfig
from kvdb.sharding.ring import HashRing

__all__ = ["HashRing", "ShardConfig", "ShardingConfig"]
