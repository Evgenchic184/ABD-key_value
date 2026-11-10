"""HTTP-роутер, скрывающий шарды за единым key-value API."""

from contextlib import asynccontextmanager
from typing import Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx

from kvdb.sharding.config import ShardingConfig
from kvdb.sharding.router import ShardRouter, ShardUnavailable


class ValueBody(BaseModel):
    """Тело записи: значение в Base64, как у обычного узла."""

    value: str


def get_router(request: Request) -> ShardRouter:
    return request.app.state.router


def unavailable_handler(request: Request, exc: ShardUnavailable) -> JSONResponse:
    return JSONResponse(status_code=503, content={
        "detail": str(exc), "shard_id": exc.shard_id, "attempts": list(exc.attempts),
    })


def create_router_app(config: ShardingConfig, *, transport: httpx.BaseTransport | None = None) -> FastAPI:
    """Собрать приложение роутера; клиент создаётся в lifespan."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.Client(timeout=config.timeout, trust_env=False, transport=transport)
        router = ShardRouter(config, client=client)
        app.state.router = router
        try:
            yield
        finally:
            router.close()
            client.close()

    app = FastAPI(
        title="KVDB shard router",
        description="Единый REST API поверх шардированного кластера KVDB.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_exception_handler(ShardUnavailable, unavailable_handler)

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "role": "router", "shards": len(config.shards)}

    @app.get("/sharding/status")
    def sharding_status(router: ShardRouter = Depends(get_router)) -> dict:
        """Топология, позиции узлов и статистика маршрутизации."""
        return router.cluster_status()

    @app.get("/sharding/locate/{key:path}")
    def locate(key: str, router: ShardRouter = Depends(get_router)) -> dict:
        """Показать шард и узлы, выбранные для ключа."""
        return router.locate(key)

    @app.get("/sharding/ring")
    def ring(router: ShardRouter = Depends(get_router)) -> dict:
        """Виртуальные узлы кольца в порядке обхода."""
        return {"ring": router.ring_summary(), "points": router.ring_points()}

    @app.get("/kv/{key:path}")
    def get_value(
        key: str,
        consistency: Literal["replica", "leader"] | None = Query(default=None),
        router: ShardRouter = Depends(get_router),
    ):
        result = router.get(key, consistency)
        return JSONResponse(status_code=result.status_code, content=result.body)

    @app.put("/kv/{key:path}")
    def put_value(key: str, body: ValueBody, router: ShardRouter = Depends(get_router)):
        result = router.set(key, body.value)
        return JSONResponse(status_code=result.status_code, content=result.body)

    @app.delete("/kv/{key:path}")
    def delete_value(key: str, router: ShardRouter = Depends(get_router)):
        result = router.delete(key)
        return JSONResponse(status_code=result.status_code, content=result.body)

    @app.post("/flush")
    def flush(router: ShardRouter = Depends(get_router)):
        """Сохранить снимок на каждом шарде; частичный сбой возвращает 503."""
        result = router.flush()
        return JSONResponse(status_code=200 if result["ok"] else 503, content=result)

    return app
