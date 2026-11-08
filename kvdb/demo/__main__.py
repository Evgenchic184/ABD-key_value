"""Run: python -m kvdb.demo --leader-url http://localhost:8000."""

import argparse
from contextlib import asynccontextmanager
import logging
import math
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import httpx
import uvicorn

from kvdb.demo.service import Demo


class ManualBody(BaseModel):
    node: str = "leader"
    method: str
    key: str
    value: str = ""


def create_demo_app(nodes, *, users=3, interval=1.0, log_dir=Path("reports/demo"), autostart=True):
    @asynccontextmanager
    async def lifespan(app):
        demo = Demo(nodes, users=users, interval=interval, log_dir=log_dir, autostart=autostart)
        app.state.demo = demo
        demo.start()
        try:
            yield
        finally:
            demo.close()

    app = FastAPI(title="KVDB database interface", lifespan=lifespan)

    app.mount("/assets", StaticFiles(directory=Path(__file__).with_name("static")), name="assets")

    @app.get("/")
    def index():
        return FileResponse(Path(__file__).with_name("index.html"))

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/state")
    def state(after: int = Query(default=0, ge=0)):
        return app.state.demo.state(after)

    @app.post("/api/replicas/{node}/{action}")
    def replication_control(node: str, action: str):
        try:
            return app.state.demo.replication_control(node, action)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/kv")
    def manual(body: ManualBody):
        try:
            return app.state.demo.manual(body.node, body.method, body.key, body.value)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/action/{action}")
    def action(action: str):
        demo = app.state.demo
        try:
            if action == "start":
                demo.set_running(True)
            elif action == "pause":
                demo.set_running(False)
            elif action == "flush":
                return demo.flush()
            elif action == "check":
                demo.begin_check()
            else:
                raise HTTPException(404, "unknown action")
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(503, str(exc)) from exc
        return {"status": "ok"}

    return app


def main(*, default_autostart=True):
    parser = argparse.ArgumentParser(description="KVDB database interface and optional workload")
    parser.add_argument("--leader-url", default="http://127.0.0.1:8000")
    parser.add_argument("--replica-urls", nargs="+", default=["http://127.0.0.1:8001", "http://127.0.0.1:8002"])
    parser.add_argument("--autostart", action=argparse.BooleanOptionalAction, default=default_autostart)
    parser.add_argument("--users", type=int, default=3)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--log-dir", type=Path, default=Path("reports/demo"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    if args.users < 1 or not math.isfinite(args.interval) or args.interval <= 0:
        parser.error("users and interval must be positive")
    urls = [args.leader_url, *args.replica_urls]
    if len(set(url.rstrip("/") for url in urls)) != len(urls):
        parser.error("node URLs must be distinct")
    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            parser.error("node URLs must be HTTP(S) origins")
    nodes = {"leader": args.leader_url.rstrip("/")}
    nodes.update({f"replica{i}": url.rstrip("/") for i, url in enumerate(args.replica_urls, 1)})
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    uvicorn.run(create_demo_app(nodes, users=args.users, interval=args.interval, log_dir=args.log_dir, autostart=args.autostart),
                host=args.host, port=args.port, access_log=False)


if __name__ == "__main__":
    main()
