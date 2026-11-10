"""Маршрутизация операций по шардам и отказоустойчивое чтение."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
import time
from urllib.parse import quote

import httpx

from kvdb.sharding.config import ShardingConfig
from kvdb.sharding.ring import HashRing


@dataclass(frozen=True)
class ProxyResult:
    """Ответ узла шарда, сохранённый для проксирования клиенту."""

    status_code: int
    body: dict
    shard_id: str
    node: str
    attempts: tuple[dict, ...] = ()


class ShardUnavailable(RuntimeError):
    """Ни один подходящий узел шарда не ответил."""

    def __init__(self, shard_id: str, attempts: list[dict]):
        self.shard_id = shard_id
        self.attempts = tuple(attempts)
        super().__init__(f"shard {shard_id} is unavailable")


def _body(response: httpx.Response) -> dict:
    try:
        body = response.json()
    except ValueError:
        return {"detail": response.text[:300]}
    return body if isinstance(body, dict) else {"detail": str(body)[:300]}


class ShardRouter:
    """Выбирает шард по кольцу и проксирует к нему HTTP-запросы.

    Записи идут только на ведущего шарда. Чтения по умолчанию обходят
    реплики по кругу, затем обращаются к ведущему. Сетевые ошибки и ответы
    5xx запускают следующую попытку; остальные ответы возвращаются клиенту
    без изменений.
    """

    def __init__(self, config: ShardingConfig, client: httpx.Client | None = None):
        self.config = config
        self.ring = HashRing([shard.id for shard in config.shards], config.virtual_nodes)
        self._shards = {shard.id: shard for shard in config.shards}
        self.client = client or httpx.Client(timeout=config.timeout, trust_env=False)
        self._owns_client = client is None
        self._lock = Lock()
        self._read_cursor: dict[str, int] = {}
        self._stats = {
            shard.id: {"requests": 0, "failures": 0, "last_error": None,
                       "last_latency_ms": None, "last_success": None}
            for shard in config.shards
        }

    def close(self):
        if self._owns_client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def shard(self, shard_id: str):
        return self._shards[shard_id]

    def owner(self, key: str) -> str:
        return self.ring.owner(key)

    def _record(self, shard_id: str, ok: bool, error: str | None, latency_ms: float):
        with self._lock:
            stats = self._stats[shard_id]
            stats["requests"] += 1
            stats["last_latency_ms"] = round(latency_ms, 1)
            if ok:
                stats["last_success"] = datetime.now(timezone.utc).isoformat()
                stats["last_error"] = None
            else:
                stats["failures"] += 1
                stats["last_error"] = error

    def stats(self, shard_id: str) -> dict:
        with self._lock:
            return self._stats[shard_id].copy()

    def _send(self, method: str, base_url: str, key: str, value: str | None = None):
        url = f"{base_url}/kv/{quote(key, safe='')}"
        started = time.monotonic()
        try:
            if method == "GET":
                response = self.client.get(url)
            elif method == "PUT":
                response = self.client.put(url, json={"value": value})
            else:
                response = self.client.delete(url)
        except (httpx.HTTPError, OSError) as exc:
            latency = (time.monotonic() - started) * 1000
            error = f"{type(exc).__name__}: {exc}"
            return None, {"detail": error}, error, latency
        latency = (time.monotonic() - started) * 1000
        return response.status_code, _body(response), None, latency

    def _read_order(self, shard_id: str, consistency: str) -> tuple[str, ...]:
        if consistency not in ("replica", "leader"):
            raise ValueError("consistency must be 'replica' or 'leader'")
        shard = self._shards[shard_id]
        if consistency == "leader" or not shard.replicas:
            return (shard.leader,)
        with self._lock:
            cursor = self._read_cursor.get(shard_id, 0)
            self._read_cursor[shard_id] = cursor + 1
        offset = cursor % len(shard.replicas)
        replicas = shard.replicas[offset:] + shard.replicas[:offset]
        return (*replicas, shard.leader)

    def _read(self, key: str, consistency: str) -> ProxyResult:
        shard_id = self.owner(key)
        attempts = []
        for node in self._read_order(shard_id, consistency):
            status, body, error, latency = self._send("GET", node, key)
            failed = error is not None or status >= 500
            self._record(shard_id, not failed, error or (f"HTTP {status}" if failed else None), latency)
            attempts.append({"node": node, "status": status, "error": error})
            if not failed:
                return ProxyResult(status, body, shard_id, node, tuple(attempts))
        raise ShardUnavailable(shard_id, attempts)

    def get(self, key: str, consistency: str | None = None) -> ProxyResult:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        return self._read(key, consistency or self.config.reads)

    def _write(self, method: str, key: str, value: str | None = None) -> ProxyResult:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        shard_id = self.owner(key)
        node = self._shards[shard_id].leader
        status, body, error, latency = self._send(method, node, key, value)
        failed = error is not None or status >= 500
        self._record(shard_id, not failed, error or (f"HTTP {status}" if failed else None), latency)
        if error is not None:
            raise ShardUnavailable(shard_id, [{"node": node, "status": None, "error": error}])
        return ProxyResult(status, body, shard_id, node)

    def set(self, key: str, value: str) -> ProxyResult:
        if not isinstance(value, str):
            raise TypeError("value must be Base64 text")
        return self._write("PUT", key, value)

    def delete(self, key: str) -> ProxyResult:
        return self._write("DELETE", key)

    def locate(self, key: str) -> dict:
        shard_id = self.owner(key)
        shard = self._shards[shard_id]
        return {"key": key, "shard_id": shard_id, "token": self.ring.token_hex(key),
                "leader": shard.leader, "replicas": list(shard.replicas),
                "read_urls": list(shard.read_urls)}

    def ring_summary(self) -> dict:
        return self.ring.summary()

    def ring_points(self) -> list[dict]:
        return self.ring.points()

    def _flush_shard(self, shard_id: str) -> dict:
        node = self._shards[shard_id].leader
        started = time.monotonic()
        try:
            response = self.client.post(node + "/flush")
            body, error = _body(response), None
        except (httpx.HTTPError, OSError) as exc:
            response, body, error = None, {"detail": str(exc)}, f"{type(exc).__name__}: {exc}"
        latency = (time.monotonic() - started) * 1000
        ok = error is None and response.status_code == 200
        self._record(shard_id, ok, error or (None if ok else f"HTTP {response.status_code}"), latency)
        return {"shard_id": shard_id, "node": node, "ok": ok,
                "status": response.status_code if response is not None else None, "body": body}

    def flush(self) -> dict:
        """Сохранить снимки на всех ведущих; операция не атомарна между шардами."""
        with ThreadPoolExecutor(max_workers=len(self._shards)) as pool:
            results = list(pool.map(self._flush_shard, self._shards))
        return {"ok": all(item["ok"] for item in results), "shards": results}

    def _probe(self, url: str) -> dict:
        try:
            response = self.client.get(url + "/replication/status")
            body = _body(response)
            return {"url": url, "available": response.status_code == 200,
                    "status": body, "error": None if response.status_code == 200
                    else f"HTTP {response.status_code}"}
        except (httpx.HTTPError, OSError) as exc:
            return {"url": url, "available": False, "status": None,
                    "error": f"{type(exc).__name__}: {exc}"}

    def cluster_status(self) -> dict:
        targets = []
        for shard in self.config.shards:
            targets.append((shard.id, shard.leader, "leader"))
            targets.extend((shard.id, url, "replica") for url in shard.replicas)
        with ThreadPoolExecutor(max_workers=max(1, len(targets))) as pool:
            probes = list(pool.map(lambda item: self._probe(item[1]), targets))
        nodes = {}
        for (shard_id, url, role), probe in zip(targets, probes):
            nodes.setdefault(shard_id, []).append({"role": role, **probe})
        shards = []
        for shard in self.config.shards:
            shards.append({**shard.wire(), "tokens": self.ring.summary()["points_per_shard"][shard.id],
                           "stats": self.stats(shard.id), "nodes": nodes.get(shard.id, [])})
        leaders_available = all(
            any(node["role"] == "leader" and node["available"] for node in item["nodes"])
            for item in shards
        )
        return {"role": "router", "reads": self.config.reads, "leaders_available": leaders_available,
                "ring": self.ring.summary(), "shards": shards}
