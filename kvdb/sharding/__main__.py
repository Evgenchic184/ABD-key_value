"""Точка входа HTTP-роутера шардированного кластера."""

import argparse
from pathlib import Path

import uvicorn

from kvdb.sharding.config import ShardingConfig
from kvdb.sharding.server import create_router_app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="HTTP-роутер шардированной key-value базы")
    parser.add_argument("--config", type=Path, required=True, help="JSON-описание шардов")
    parser.add_argument("--host", default="127.0.0.1", help="адрес привязки")
    parser.add_argument("--port", type=int, default=8000, help="порт привязки")
    args = parser.parse_args(argv)
    try:
        config = ShardingConfig.from_file(args.config)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    uvicorn.run(create_router_app(config), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
