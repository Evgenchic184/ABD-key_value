"""Run: python -m kvdb.demo --leader-url http://localhost:8000."""

import argparse
from contextlib import asynccontextmanager
import hashlib
import logging
import math
from pathlib import Path
from urllib.parse import urlparse
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import httpx
import uvicorn

from kvdb.demo.service import Demo


class ManualBody(BaseModel):
    target: Literal["leader", "node"] = "node"
    node: str | None = None
    method: str
    key: str
    value: str = ""


def create_demo_app(nodes, *, users=3, interval=1.0, log_dir=Path("reports/demo"), autostart=True):
    assets = Path(__file__).with_name("static")
    asset_version = hashlib.sha256(
        (assets / "dashboard.js").read_bytes() + (assets / "style.css").read_bytes()
    ).hexdigest()[:12]

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

    @app.middleware("http")
    async def cache_control(request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/")
    def index():
        html = Path(__file__).with_name("index.html").read_text(encoding="utf-8")
        for name in ("dashboard.js", "style.css"):
            html = html.replace(f"/assets/{name}", f"/assets/{name}?v={asset_version}")
        return HTMLResponse(html)

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
            return app.state.demo.manual(body.node, body.method, body.key, body.value, target=body.target)
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
    parser.add_argument("--nodes", nargs="+", metavar="NAME=URL")
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
    if args.nodes:
        nodes = {}
        for entry in args.nodes:
            name, separator, url = entry.partition("=")
            if not separator or not name or name in nodes:
                parser.error("--nodes requires unique NAME=URL entries")
            nodes[name] = url.rstrip("/")
    else:
        nodes = {f"node{i}": url.rstrip("/") for i, url in
                 enumerate([args.leader_url, *args.replica_urls], 1)}
    urls = list(nodes.values())
    if len(set(url.rstrip("/") for url in urls)) != len(urls):
        parser.error("node URLs must be distinct")
    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            parser.error("node URLs must be HTTP(S) origins")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    uvicorn.run(create_demo_app(nodes, users=args.users, interval=args.interval, log_dir=args.log_dir, autostart=args.autostart),
                host=args.host, port=args.port, access_log=False)


if __name__ == "__main__":
    main()
