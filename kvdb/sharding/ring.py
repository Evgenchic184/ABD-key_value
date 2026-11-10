"""Стабильное кольцо consistent hashing для выбора шарда по ключу."""

from bisect import bisect_left
import hashlib


def _hash(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest(), "big")


class HashRing:
    """Кольцо из виртуальных узлов; ключ закрепляется за следующим узлом.

    Удаление шарда затрагивает только его диапазоны, а добавление нового
    забирает часть диапазонов у существующих шардов. Раскладка не зависит
    от порядка шардов в конфигурации и восстанавливается после перезапуска.
    """

    def __init__(self, shard_ids, virtual_nodes: int = 128):
        shard_ids = tuple(shard_ids)
        if not shard_ids:
            raise ValueError("hash ring requires at least one shard")
        if len(set(shard_ids)) != len(shard_ids):
            raise ValueError("hash ring shard ids must be unique")
        if type(virtual_nodes) is not int or virtual_nodes < 1:
            raise ValueError("virtual_nodes must be a positive integer")
        points = [(_hash(f"shard:{shard_id}:{index}"), shard_id)
                  for shard_id in shard_ids for index in range(virtual_nodes)]
        points.sort()
        self.shard_ids = shard_ids
        self.virtual_nodes = virtual_nodes
        self._tokens = [token for token, _ in points]
        self._owners = [shard_id for _, shard_id in points]

    @staticmethod
    def key_token(key: str) -> int:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        return _hash(f"key:{key}")

    def owner(self, key: str) -> str:
        """Шард, которому принадлежит ключ."""
        token = self.key_token(key)
        index = bisect_left(self._tokens, token)
        if index == len(self._tokens):
            index = 0
        return self._owners[index]

    def token_hex(self, key: str) -> str:
        """Позиция ключа на кольце для диагностики."""
        return format(self.key_token(key), "064x")

    def points(self) -> list[dict]:
        """Все виртуальные узлы в порядке обхода кольца."""
        return [{"token": format(token, "064x"), "shard_id": owner}
                for token, owner in zip(self._tokens, self._owners)]

    def summary(self) -> dict:
        counts = {shard_id: 0 for shard_id in self.shard_ids}
        for owner in self._owners:
            counts[owner] += 1
        return {"algorithm": "consistent-hash-sha256", "virtual_nodes": self.virtual_nodes,
                "points": len(self._tokens), "points_per_shard": counts}
