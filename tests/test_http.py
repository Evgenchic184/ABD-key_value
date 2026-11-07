"""Проверки HTTP-сервера: REST-операции, кодирование значений и ошибки."""

import base64
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote
from unittest.mock import patch

import httpx
import pytest
from fastapi.testclient import TestClient

from kvdb import FileStorage, KVEngine
from kvdb.http.server import create_app

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        yield client


def encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_crud_and_missing_key(client):
    response = client.put("/kv/my%20key", json={"value": encode(b"hello world")})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    response = client.get("/kv/my%20key")
    assert response.status_code == 200
    assert response.json() == {"key": "my key", "value": encode(b"hello world")}

    response = client.put("/kv/my%20key", json={"value": encode(b"replaced")})
    assert response.status_code == 200
    replaced = base64.b64decode(client.get("/kv/my%20key").json()["value"])
    assert replaced == b"replaced"

    missing = client.get("/kv/absent")
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Ключ не найден"}

    assert client.delete("/kv/my%20key").status_code == 200
    assert client.get("/kv/my%20key").status_code == 404


def test_binary_values_roundtrip(client):
    binary = bytes(range(256))
    assert client.put("/kv/binary", json={"value": encode(binary)}).status_code == 200
    response = client.get("/kv/binary")
    assert response.status_code == 200
    assert base64.b64decode(response.json()["value"]) == binary


def test_empty_key_and_value(client):
    assert client.put("/kv/", json={"value": ""}).status_code == 200
    response = client.get("/kv/")
    assert response.status_code == 200
    assert response.json() == {"key": "", "value": ""}


def test_unicode_key(client):
    path = "/kv/" + quote("ключ")
    assert client.put(path, json={"value": encode("значение".encode())}).status_code == 200
    response = client.get(path)
    assert response.status_code == 200
    assert response.json() == {"key": "ключ", "value": encode("значение".encode())}


def test_key_with_slash(client):
    assert client.put("/kv/dir/item", json={"value": encode(b"nested")}).status_code == 200
    response = client.get("/kv/dir/item")
    assert response.status_code == 200
    assert response.json()["key"] == "dir/item"


def test_delete_absent_key_is_idempotent(client):
    assert client.delete("/kv/never-existed").status_code == 200


def test_invalid_base64_is_rejected(client):
    response = client.put("/kv/bad", json={"value": "not base64!!!"})
    assert response.status_code == 400
    assert "Base64" in response.json()["detail"]
    assert client.get("/kv/bad").status_code == 404


def test_invalid_body_is_rejected(client):
    assert client.put("/kv/key", json={}).status_code == 422
    assert client.put("/kv/key", json={"value": 42}).status_code == 422
    assert client.get("/kv/key").status_code == 404


def test_flush_writes_snapshot_and_clears_wal(client, tmp_path):
    client.put("/kv/flushed", json={"value": encode(b"value")})
    assert client.post("/flush").status_code == 200
    assert FileStorage(tmp_path / "snapshot.json").load() == {"flushed": b"value"}
    assert (tmp_path / "wal.bin").read_bytes() == b""


def test_data_survives_restart(tmp_path):
    with TestClient(create_app(tmp_path)) as first:
        first.put("/kv/persist", json={"value": encode(b"saved")})
        first.put("/kv/removed", json={"value": encode(b"gone")})
        assert first.delete("/kv/removed").status_code == 200
    with TestClient(create_app(tmp_path)) as second:
        response = second.get("/kv/persist")
        assert response.status_code == 200
        assert base64.b64decode(response.json()["value"]) == b"saved"
        assert second.get("/kv/removed").status_code == 404


def test_closed_engine_returns_503(client):
    with patch.object(KVEngine, "get", side_effect=RuntimeError("База закрыта")):
        response = client.get("/kv/key")
    assert response.status_code == 503
    assert response.json() == {"detail": "База закрыта"}


def test_storage_error_returns_500(client):
    with patch.object(KVEngine, "set", side_effect=OSError("disk full")):
        response = client.put("/kv/key", json={"value": encode(b"value")})
    assert response.status_code == 500
    assert response.json() == {"detail": "disk full"}


def test_uvicorn_entrypoint_serves_and_persists(tmp_path):
    with socket.socket() as picker:
        picker.bind(("127.0.0.1", 0))
        port = picker.getsockname()[1]
    process = subprocess.Popen(
        [
            sys.executable, "-m", "kvdb.http",
            "--data-dir", str(tmp_path), "--host", "127.0.0.1", "--port", str(port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=1) as http:
            response = None
            for _ in range(100):
                try:
                    response = http.get("/health")
                    break
                except httpx.TransportError:
                    if process.poll() is not None:
                        raise AssertionError("сервер завершился до готовности")
                    time.sleep(0.05)
            assert response is not None and response.status_code == 200
            assert http.put(
                "/kv/deployed", json={"value": encode(b"hosted")}
            ).status_code == 200
            assert base64.b64decode(http.get("/kv/deployed").json()["value"]) == b"hosted"
    finally:
        process.terminate()
        process.wait(timeout=10)
    assert FileStorage(tmp_path / "snapshot.json").load() == {"deployed": b"hosted"}
