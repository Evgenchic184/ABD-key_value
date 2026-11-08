"""Validated server configuration."""

from dataclasses import dataclass
import math
from urllib.parse import urlparse


@dataclass(frozen=True)
class ReplicationConfig:
    role: str = "standalone"
    group_id: str | None = None
    leader_url: str | None = None
    interval: float = 0.5
    timeout: float = 2.0

    def __post_init__(self):
        if self.role not in ("standalone", "leader", "replica"):
            raise ValueError("unknown role")
        if self.role != "standalone" and (not isinstance(self.group_id, str) or not self.group_id.strip()):
            raise ValueError("leader and replica require --group-id")
        if self.role == "standalone" and (self.group_id or self.leader_url):
            raise ValueError("standalone does not accept cluster arguments")
        if self.role == "replica":
            parsed = urlparse(self.leader_url or "")
            if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
                raise ValueError("replica requires an HTTP(S) --leader-url")
        elif self.leader_url:
            raise ValueError("only replicas accept --leader-url")
        if not all(math.isfinite(v) and v > 0 for v in (self.interval, self.timeout)):
            raise ValueError("replication interval and timeout must be positive and finite")
