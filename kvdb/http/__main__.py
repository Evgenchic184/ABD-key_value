"""Точка входа HTTP-сервера."""

import argparse
from pathlib import Path

import uvicorn

from kvdb.http.server import create_app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="HTTP-сервер key-value базы")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="каталог базы")
    parser.add_argument("--host", default="127.0.0.1", help="адрес привязки")
    parser.add_argument("--port", type=int, default=8000, help="порт привязки")
    args = parser.parse_args(argv)
    uvicorn.run(create_app(args.data_dir), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
