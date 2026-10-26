"""HTTP-адаптер к публичному API базы."""

import base64
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvdb import FileStorage, FileWAL, KVEngine


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


def create_app(data_dir: Path) -> FastAPI:
    """Собрать приложение, обслуживающее один каталог базы.

    Движок открывается на старте сервера и закрывается с сохранением данных
    при остановке. Операции объявлены синхронными: FastAPI выполняет их
    в пуле потоков, порядок записей обеспечивает блокировка движка.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = KVEngine(
            FileStorage(data_dir / "snapshot.json"), FileWAL(data_dir / "wal.bin")
        )
        app.state.engine = engine
        try:
            yield
        finally:
            engine.close()

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
