"""HTTP-адаптер к публичному API базы."""

import base64
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvdb import FileStorage, FileWAL, KVEngine
from kvdb._files import DirectoryLock
from kvdb.replication.config import ReplicationConfig
from kvdb.replication.engine import ReplicatedEngine
from kvdb.replication.models import ReadOnlyError, ReplicationConflict
from kvdb.replication.worker import ReplicationWorker


class ValueBody(BaseModel):
    """Тело запроса записи: значение в Base64."""

    value: str


def get_engine(request: Request) -> KVEngine:
    """Экземпляр движка, открытый в lifespan приложения."""
    return request.app.state.engine


def runtime_error_handler(request: Request, exc: RuntimeError) -> JSONResponse:
    """Закрытая или заблокированная после ошибки база недоступна."""
    return JSONResponse(status_code=503, content={"detail": str(exc)})


def storage_error_handler(request: Request, exc: OSError) -> JSONResponse:
    """Ошибка дисковой операции движка."""
    return JSONResponse(status_code=500, content={"detail": str(exc)})


def create_app(data_dir: Path, config: ReplicationConfig | None = None) -> FastAPI:
    """Собрать приложение, обслуживающее один каталог базы.

    Движок открывается на старте сервера и закрывается с сохранением данных
    при остановке. Операции объявлены синхронными: FastAPI выполняет их
    в пуле потоков, порядок записей обеспечивает блокировка движка.
    """

    config = config or ReplicationConfig()
    data_dir = Path(data_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        directory_lock = DirectoryLock(data_dir)
        engine = worker = None
        try:
            storage = FileStorage(data_dir / "snapshot.json")
            wal = FileWAL(data_dir / "wal.bin")
            if config.role == "standalone":
                engine = KVEngine(storage, wal)
            else:
                engine = ReplicatedEngine(storage, wal, role=config.role, group_id=config.group_id)
            app.state.engine = engine
            app.state.worker = None
            if config.role == "replica":
                worker = ReplicationWorker(engine, config)
                app.state.worker = worker
                worker.start()
            yield
        finally:
            try:
                if worker:
                    worker.close()
                if engine:
                    engine.close()
            finally:
                directory_lock.close()

    app = FastAPI(
        title="KVDB",
        description=(
            "REST-доступ к key-value базе: строковые ключи, байтовые значения "
            "в Base64."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_exception_handler(RuntimeError, runtime_error_handler)
    app.add_exception_handler(OSError, storage_error_handler)

    @app.exception_handler(ReadOnlyError)
    def read_only(request, exc):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(ReplicationConflict)
    def conflict(request, exc):
        return JSONResponse(status_code=409, content={"code": exc.code})

    @app.get("/replication/status")
    def replication_status(request: Request):
        if config.role == "standalone":
            return {"role": "standalone", "ready": True}
        result = request.app.state.engine.status()
        if request.app.state.worker:
            result.update(request.app.state.worker.status())
        else:
            result["state"] = "leader" if result["ready"] else "error"
        return result

    @app.post("/internal/replication/{action}")
    def replication_control(action: str, request: Request):
        if config.role != "replica":
            raise HTTPException(403, "Only replicas have a synchronization worker")
        if action not in ("pause", "resume"):
            raise HTTPException(404, "Unknown replication action")
        return request.app.state.worker.set_paused(action == "pause")

    def leader_engine(request: Request):
        if config.role != "leader":
            raise HTTPException(status_code=403, detail="Only the leader serves replication")
        return request.app.state.engine

    @app.get("/internal/replication/status")
    def internal_status(engine=Depends(leader_engine)):
        return engine.status()

    @app.get("/internal/replication/changes")
    def changes(after: int = Query(ge=0), limit: int = Query(default=1000, ge=1, le=1000),
                engine=Depends(leader_engine)):
        return engine.changes(after, limit)

    @app.get("/internal/replication/snapshot")
    def snapshot(engine=Depends(leader_engine)):
        return engine.export_snapshot()

    @app.get("/health")
    def health() -> dict:
        """Проверка доступности сервера."""
        return {"status": "ok"}

    @app.get("/kv/{key:path}")
    def get_value(key: str, engine: KVEngine = Depends(get_engine)) -> dict:
        """Получить значение по ключу."""
        value = engine.get(key)
        if value is None:
            raise HTTPException(status_code=404, detail="Ключ не найден")
        return {"key": key, "value": base64.b64encode(value).decode("ascii")}

    @app.put("/kv/{key:path}")
    def put_value(
        key: str, body: ValueBody, engine: KVEngine = Depends(get_engine)
    ) -> dict:
        """Записать или заменить значение."""
        try:
            value = base64.b64decode(body.value.encode("ascii"), validate=True)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=f"Значение должно быть строкой Base64: {exc}"
            ) from exc
        engine.set(key, value)
        return {"status": "ok"}

    @app.delete("/kv/{key:path}")
    def delete_value(key: str, engine: KVEngine = Depends(get_engine)) -> dict:
        """Удалить ключ; отсутствие ключа ошибкой не считается."""
        engine.delete(key)
        return {"status": "ok"}

    @app.post("/flush")
    def flush(engine: KVEngine = Depends(get_engine)) -> dict:
        """Сохранить снимок и очистить журнал."""
        engine.flush()
        return {"status": "ok"}

    return app
