"""Точка входа HTTP-сервера."""

import argparse
from pathlib import Path

import uvicorn

from kvdb.http.server import create_app
from kvdb.replication.config import ReplicationConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="HTTP-сервер key-value базы")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="каталог базы")
    parser.add_argument("--host", default="127.0.0.1", help="адрес привязки")
    parser.add_argument("--port", type=int, default=8000, help="порт привязки")
    parser.add_argument("--role", choices=("standalone", "leader", "replica"), default="standalone")
    parser.add_argument("--group-id")
    parser.add_argument("--leader-url")
    parser.add_argument("--replication-interval", type=float, default=0.5)
    parser.add_argument("--replication-timeout", type=float, default=2.0)
    args = parser.parse_args(argv)
    try:
        config = ReplicationConfig(args.role, args.group_id, args.leader_url,
                                   args.replication_interval, args.replication_timeout)
    except ValueError as exc:
        parser.error(str(exc))
    uvicorn.run(create_app(args.data_dir, config), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
