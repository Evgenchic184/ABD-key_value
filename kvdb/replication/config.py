"""Validated server configuration."""

from dataclasses import dataclass
import math
import re
from urllib.parse import urlparse


@dataclass(frozen=True)
class ReplicationConfig:
    role: str = "standalone"
    group_id: str | None = None
    leader_url: str | None = None
    interval: float = 0.5
    timeout: float = 2.0
    zookeeper_hosts: str | None = None
    node_id: str | None = None
    advertise_url: str | None = None
    session_timeout: float = 6.0

    def __post_init__(self):
        if self.role not in ("standalone", "leader", "replica"):
            raise ValueError("unknown role")
        if self.role != "standalone" and (not isinstance(self.group_id, str) or not self.group_id.strip()):
            raise ValueError("leader and replica require --group-id")
        if self.role == "standalone" and (self.group_id or self.leader_url or self.zookeeper_hosts):
            raise ValueError("standalone does not accept cluster arguments")
        if not all(math.isfinite(v) and v > 0 for v in (self.interval, self.timeout)):
            raise ValueError("replication interval and timeout must be positive and finite")
        if self.zookeeper_hosts:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", self.group_id):
                raise ValueError("ZooKeeper group must contain ASCII letters, digits, _ or -")
            if not self.node_id or not re.fullmatch(r"[A-Za-z0-9_-]+", self.node_id):
                raise ValueError("ZooKeeper requires an ASCII --node-id")
            if self.leader_url:
                raise ValueError("ZooKeeper discovers the leader; omit --leader-url")
            if not math.isfinite(self.session_timeout) or self.session_timeout < 2:
                raise ValueError("session timeout must be at least 2 seconds")
            url, option = self.advertise_url, "--advertise-url"
        else:
            if self.node_id or self.advertise_url:
                raise ValueError("node identity requires --zookeeper-hosts")
            if self.role != "replica":
                if self.leader_url:
                    raise ValueError("only replicas accept --leader-url")
                return
            url, option = self.leader_url, "--leader-url"
        parsed = urlparse(url or "")
        if (parsed.scheme not in ("http", "https") or not parsed.netloc
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise ValueError(f"cluster requires an HTTP(S) {option}")
