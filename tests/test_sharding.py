"""Проверки кольца, конфигурации, маршрутизации и сетевого шардирования."""

import base64
import json
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import quote, unquote

from fastapi.testclient import TestClient
import httpx
import pytest

from kvdb.sharding.config import ShardConfig, ShardingConfig
from kvdb.sharding.ring import HashRing
from kvdb.sharding.router import ShardRouter, ShardUnavailable
from kvdb.sharding.server import create_router_app

ROOT = Path(__file__).resolve().parents[1]


def config(*, reads="replica", replicas=True):
    return ShardingConfig((
        ShardConfig("a", "http://a-leader", ("http://a-r1", "http://a-r2") if replicas else ()),
        ShardConfig("b", "http://b-leader", ("http://b-r1",) if replicas else ()),
    ), virtual_nodes=32, timeout=0.2, reads=reads)


def key_for(router, shard_id):
    for number in range(10000):
        key = f"key-{number}"
        if router.owner(key) == shard_id:
            return key
    raise AssertionError("no key for shard")


class FakeNodes:
    """Мини-кластер узлов за одним MockTransport."""

    def __init__(self):
        self.data, self.calls = {}, []
        self.connect_errors, self.status_overrides, self.flushes = set(), {}, []

    def __call__(self, request):
        host, path = request.url.host, request.url.path
        self.calls.append((host, request.method, path))
        if host in self.connect_errors:
            raise httpx.ConnectError("connection refused", request=request)
        if host in self.status_overrides:
            return httpx.Response(self.status_overrides[host], json={"detail": "injected"})
        if path == "/flush":
            self.flushes.append(host)
            return httpx.Response(200, json={"status": "ok"})
        if path == "/replication/status":
            return httpx.Response(200, json={
                "role": "leader" if host.endswith("leader") else "replica",
                "group_id": host.split("-")[0], "ready": True, "last_seq": 1,
            })
        if not path.startswith("/kv/"):
            return httpx.Response(404, json={"detail": "not found"})
        key = unquote(path[4:])
        if request.method == "PUT":
            value = json.loads(request.content)["value"]
            try:
                base64.b64decode(value.encode("ascii"), validate=True)
            except ValueError:
                return httpx.Response(400, json={"detail": "invalid base64"})
            self.data.setdefault(host, {})[key] = value
            return httpx.Response(200, json={"status": "ok"})
        if request.method == "DELETE":
            self.data.setdefault(host, {}).pop(key, None)
            return httpx.Response(200, json={"status": "ok"})
        if key in self.data.get(host, {}):
            return httpx.Response(200, json={"key": key, "value": self.data[host][key]})
        return httpx.Response(404, json={"detail": "not found"})


def make_router(nodes, cfg=None):
    client = httpx.Client(transport=httpx.MockTransport(nodes))
    return ShardRouter(cfg or config(), client=client)


def test_ring_is_deterministic_balanced_and_stable():
    ring = HashRing(["a", "b", "c"], virtual_nodes=128)
    owners = [ring.owner(f"user-{number}") for number in range(10000)]
    counts = {shard: owners.count(shard) for shard in ring.shard_ids}
    assert all(2000 < count < 5000 for count in counts.values())
    assert [ring.owner(f"user-{number}") for number in range(10000)] == owners
    assert ring.owner("") == HashRing(["a", "b", "c"], virtual_nodes=128).owner("")
    assert ring.owner("ключ/с пробелом") in ring.shard_ids
    assert len(ring.points()) == 3 * 128
    assert [point["token"] for point in ring.points()] == sorted(p["token"] for p in ring.points())
    assert ring.summary()["points_per_shard"] == {shard: 128 for shard in ring.shard_ids}

    without_c = HashRing(["a", "b"], virtual_nodes=128)
    for key in (f"user-{number}" for number in range(3000)):
        if ring.owner(key) != "c":
            assert without_c.owner(key) == ring.owner(key)

    added = HashRing(["a", "b", "c", "d"], virtual_nodes=128)
    for key in (f"user-{number}" for number in range(3000)):
        if added.owner(key) != "d":
            assert added.owner(key) == ring.owner(key)


def test_ring_rejects_invalid_membership():
    with pytest.raises(ValueError):
        HashRing([])
    with pytest.raises(ValueError):
        HashRing(["a", "a"])
    with pytest.raises(ValueError):
        HashRing(["a"], virtual_nodes=0)
    with pytest.raises(TypeError):
        HashRing(["a"]).owner(1)


def test_config_validation_and_wire():
    cfg = config()
    assert cfg.shards[0].read_urls == ("http://a-r1", "http://a-r2")
    assert ShardConfig("a", "http://host/").leader == "http://host"
    assert ShardingConfig.from_mapping(cfg.wire()) == cfg
    assert ShardingConfig.from_mapping({"shards": [{"id": "a", "leader": "http://a"}]}).reads == "replica"

    invalid = [
        {"shards": []},
        {"shards": [{"id": "a", "leader": "http://a"}, {"id": "a", "leader": "http://b"}]},
        {"shards": [{"id": "a", "leader": "http://a", "replicas": ["http://a"]}]},
        {"shards": [{"id": "a", "leader": "http://a"}, {"id": "b", "leader": "http://a"}]},
        {"shards": [{"id": "", "leader": "http://a"}]},
        {"shards": [{"id": "a", "leader": "http://a/path"}]},
        {"shards": [{"id": "a", "leader": "ftp://a"}]},
        {"shards": [{"id": "a", "leader": "http://a:bad"}]},
        {"shards": [{"id": "a", "leader": "http://:8000"}]},
        {"shards": [{"id": "a", "leader": "http://a:"}]},
        {"shards": [{"id": "a", "leader": "http://a b"}]},
        {"shards": [{"id": "a", "leader": "http://a"}], "virtual_nodes": 0},
        {"shards": [{"id": "a", "leader": "http://a"}], "virtual_nodes": True},
        {"shards": [{"id": "a", "leader": "http://a"}], "timeout": 0},
        {"shards": [{"id": "a", "leader": "http://a"}], "reads": "nearest"},
        {"shards": [{"id": "a", "leader": "http://a"}], "unknown": 1},
        {"shards": [{"id": "a", "leader": "http://a", "extra": 1}]},
        {"shards": [{"id": "a", "leader": "http://a", "replicas": "http://r"}]},
    ]
    for raw in invalid:
        with pytest.raises(ValueError):
            ShardingConfig.from_mapping(raw)


def test_config_file(tmp_path):
    path = tmp_path / "shards.json"
    path.write_text(json.dumps(config().wire()), encoding="utf-8")
    assert ShardingConfig.from_file(path) == config()
    path.write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON"):
        ShardingConfig.from_file(path)


def test_router_writes_to_leaders_and_reads_replicas_in_rotation():
    nodes = FakeNodes()
    router = make_router(nodes)
    key_a, key_b = key_for(router, "a"), key_for(router, "b")

    assert router.set(key_a, "dg==").node == "http://a-leader"
    assert nodes.data["a-leader"][key_a] == "dg=="
    nodes.data.setdefault("a-r1", {})[key_a] = "dg=="
    nodes.data.setdefault("a-r2", {})[key_a] = "dg=="
    first, second = router.get(key_a), router.get(key_a)
    assert first.node == "http://a-r1" and second.node == "http://a-r2"
    assert router.get(key_a, "leader").node == "http://a-leader"

    assert router.set(key_b, "eA==").node == "http://b-leader"
    nodes.data.setdefault("b-r1", {})[key_b] = "eA=="
    assert router.get(key_b).node == "http://b-r1"
    assert router.delete(key_b).node == "http://b-leader"
    assert key_b not in nodes.data["b-leader"]

    located = router.locate(key_a)
    assert located["shard_id"] == "a" and located["leader"] == "http://a-leader"
    assert located["replicas"] == ["http://a-r1", "http://a-r2"]
    assert located["token"] == router.ring.token_hex(key_a)


def test_router_failover_and_error_semantics():
    nodes = FakeNodes()
    router = make_router(nodes)
    key = key_for(router, "a")
    nodes.data.setdefault("a-leader", {})[key] = "dg=="
    nodes.status_overrides.update({"a-r1": 503, "a-r2": 502})
    result = router.get(key)
    assert result.status_code == 200 and result.node == "http://a-leader"
    assert [attempt["status"] for attempt in result.attempts] == [503, 502, 200]
    assert router.stats("a")["failures"] == 2

    nodes.status_overrides["a-leader"] = 503
    with pytest.raises(ShardUnavailable) as error:
        router.get(key)
    assert error.value.shard_id == "a" and len(error.value.attempts) == 3
    nodes.status_overrides.clear()

    before = sum(1 for host, _, _ in nodes.calls if host == "a-leader")
    nodes.connect_errors.add("a-leader")
    with pytest.raises(ShardUnavailable):
        router.set(key, "dg==")
    assert sum(1 for host, _, _ in nodes.calls if host == "a-leader") == before + 1
    nodes.connect_errors.clear()

    nodes.status_overrides["a-leader"] = 500
    assert router.set(key, "dg==").status_code == 500
    nodes.status_overrides.clear()

    missing = key_for(router, "b")
    result = router.get(missing)
    assert result.status_code == 404 and result.node == "http://b-r1"
    assert not [call for call in nodes.calls if call[0] == "b-leader" and call[2].startswith("/kv/")]


def test_router_leader_only_reads_and_invalid_consistency():
    nodes = FakeNodes()
    router = make_router(nodes, config(reads="leader"))
    key = key_for(router, "a")
    nodes.data.setdefault("a-leader", {})[key] = "dg=="
    assert router.get(key).node == "http://a-leader"
    assert not [call for call in nodes.calls if call[0].startswith("a-r")]
    with pytest.raises(ValueError):
        router.get(key, "nearest")


def test_flush_all_shards_and_partial_failure():
    nodes = FakeNodes()
    router = make_router(nodes)
    result = router.flush()
    assert result["ok"] and sorted(nodes.flushes) == ["a-leader", "b-leader"]
    nodes.status_overrides["b-leader"] = 503
    result = router.flush()
    assert not result["ok"] and result["shards"][1]["status"] == 503


def test_cluster_status_reports_nodes_ring_and_stats():
    nodes = FakeNodes()
    router = make_router(nodes)
    key = key_for(router, "a")
    router.set(key, "dg==")
    nodes.status_overrides["a-r2"] = 503
    status = router.cluster_status()
    assert status["role"] == "router" and status["leaders_available"]
    assert status["ring"]["points_per_shard"] == {"a": 32, "b": 32}
    shard_a = next(item for item in status["shards"] if item["id"] == "a")
    assert shard_a["stats"]["requests"] == 1
    assert [node["available"] for node in shard_a["nodes"]] == [True, True, False]


def test_http_router_proxies_api_and_diagnostics():
    nodes = FakeNodes()
    with TestClient(create_router_app(config(), transport=httpx.MockTransport(nodes))) as client:
        assert client.get("/health").json()["role"] == "router"
        assert client.put("/kv/hello", json={"value": "dg=="}).status_code == 200
        assert client.get("/kv/hello?consistency=leader").json()["value"] == "dg=="
        assert client.get("/kv/hello?consistency=nearest").status_code == 422
        assert client.put("/kv/bad", json={"value": "!"}).status_code == 400
        assert client.put("/kv/bad", json={}).status_code == 422

        located = client.get("/sharding/locate/hello").json()
        assert located["shard_id"] in ("a", "b")
        assert located["leader"] == f"http://{located['shard_id']}-leader"
        ring = client.get("/sharding/ring").json()
        assert ring["ring"]["points"] == 64 and len(ring["points"]) == 64
        status = client.get("/sharding/status").json()
        assert status["leaders_available"] and len(status["shards"]) == 2
        assert client.post("/flush").status_code == 200
        assert client.delete("/kv/hello").status_code == 200

        for key in ("a/b", "/", "ключ с пробелом", ""):
            path = quote(key, safe="")
            assert client.put(f"/kv/{path}", json={"value": "dg=="}).status_code == 200
            assert client.get(f"/kv/{path}?consistency=leader").json()["value"] == "dg=="
            assert client.get("/sharding/locate/" + path).json()["key"] == key

        nodes.status_overrides.update({f"{located['shard_id']}-leader": 503})
        response = client.get("/kv/hello?consistency=leader")
        assert response.status_code == 503
        assert response.json()["shard_id"] == located["shard_id"]


def _free_port():
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return stream.getsockname()[1]


def _wait_health(url, process):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise AssertionError(f"node exited: {output}")
        try:
            if httpx.get(url + "/health", timeout=0.3).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise AssertionError(f"node did not start: {url}")


def _stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def test_router_entrypoint_reports_unreachable_shards(tmp_path):
    port = _free_port()
    path = tmp_path / "shards.json"
    path.write_text(json.dumps({
        "virtual_nodes": 8, "timeout": 0.1,
        "shards": [{"id": "a", "leader": "http://127.0.0.1:1"}],
    }), encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, "-m", "kvdb.sharding", "--config", str(path),
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        _wait_health(f"http://127.0.0.1:{port}", process)
        status = httpx.get(f"http://127.0.0.1:{port}/sharding/status", timeout=5).json()
        assert status["leaders_available"] is False
        assert status["shards"][0]["nodes"][0]["available"] is False
    finally:
        _stop(process)


def test_sharded_cluster_over_real_http(tmp_path):
    """Два настоящих узла получают только ключи своего шарда через роутер."""
    processes = []
    ports = [_free_port(), _free_port()]
    try:
        for index, port in enumerate(ports):
            process = subprocess.Popen(
                [sys.executable, "-m", "kvdb.http", "--data-dir", str(tmp_path / f"node-{index}"),
                 "--host", "127.0.0.1", "--port", str(port)],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            processes.append(process)
            _wait_health(f"http://127.0.0.1:{port}", process)

        cfg = ShardingConfig((
            ShardConfig("a", f"http://127.0.0.1:{ports[0]}"),
            ShardConfig("b", f"http://127.0.0.1:{ports[1]}"),
        ), virtual_nodes=32, timeout=2.0)
        ring = HashRing(["a", "b"], 32)
        key_a = next(f"key-{n}" for n in range(10000) if ring.owner(f"key-{n}") == "a")
        key_b = next(f"key-{n}" for n in range(10000) if ring.owner(f"key-{n}") == "b")

        slash_key = key_a + "/вложенный"
        with TestClient(create_router_app(cfg)) as client:
            assert client.put(f"/kv/{key_a}", json={"value": "YQ=="}).status_code == 200
            assert client.put(f"/kv/{key_b}", json={"value": "Yg=="}).status_code == 200
            assert client.put(f"/kv/{quote(slash_key, safe='')}", json={"value": "cw=="}).status_code == 200
            assert client.get(f"/kv/{key_a}?consistency=leader").json()["value"] == "YQ=="
            assert client.get(f"/kv/{key_b}?consistency=leader").json()["value"] == "Yg=="
            assert client.get(f"/kv/{quote(slash_key, safe='')}?consistency=leader").json()["value"] == "cw=="
            assert client.get("/sharding/status").json()["leaders_available"] is True
            assert client.post("/flush").status_code == 200

        assert httpx.get(f"http://127.0.0.1:{ports[0]}/kv/{key_a}").status_code == 200
        assert httpx.get(f"http://127.0.0.1:{ports[0]}/kv/{quote(slash_key, safe='')}").status_code == 200
        assert httpx.get(f"http://127.0.0.1:{ports[0]}/kv/{key_b}").status_code == 404
        assert httpx.get(f"http://127.0.0.1:{ports[1]}/kv/{key_b}").status_code == 200
        assert httpx.get(f"http://127.0.0.1:{ports[1]}/kv/{key_a}").status_code == 404
        for index in range(2):
            snapshot = json.loads((tmp_path / f"node-{index}" / "snapshot.json").read_text())
            assert snapshot["version"] == 1
            assert (key_a in snapshot["data"]) != (key_b in snapshot["data"])
    finally:
        for process in processes:
            _stop(process)
