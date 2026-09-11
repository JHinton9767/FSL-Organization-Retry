from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass, fields
from pathlib import Path

from src.path_config import ROOT
from src.sqlCompile import DEFAULT_OUTPUT_PATH
from src.sqlCompile_cohort import DEFAULT_MANUAL_STATUS_PATH, DEFAULT_ZERO_MEMBER_PERIODS_PATH
from src.sqlCompile_dashboard import DEFAULT_DUPLICATE_NAME_RECHECK_PATH, DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH


DEFAULT_HOST_CONFIG = ROOT / "config" / "sqlCompile_host.json"


@dataclass(frozen=True)
class HostConfig:
    address: str = "0.0.0.0"
    port: int = 8502
    database: Path = DEFAULT_OUTPUT_PATH
    manual_status: Path = DEFAULT_MANUAL_STATUS_PATH
    name_choices: Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH
    name_rechecks: Path = DEFAULT_DUPLICATE_NAME_RECHECK_PATH
    zero_member_periods: Path = DEFAULT_ZERO_MEMBER_PERIODS_PATH

    @property
    def data_paths(self) -> tuple[Path, ...]:
        return (self.database, self.manual_status, self.name_choices, self.name_rechecks, self.zero_member_periods)


def shared_mode() -> bool:
    return os.environ.get("FSL_DASHBOARD_SHARED") == "1"


def load_host_config(path: str | Path | None = None) -> HostConfig:
    configured = path or os.environ.get("FSL_DASHBOARD_HOST_CONFIG")
    source = Path(configured) if configured else DEFAULT_HOST_CONFIG
    if not source.is_absolute():
        source = ROOT / source
    if not source.exists():
        if configured:
            raise FileNotFoundError(f"Host configuration not found: {source}")
        return HostConfig()
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Host configuration must be a JSON object.")
    unknown = set(payload) - {field.name for field in fields(HostConfig)}
    if unknown:
        raise ValueError(f"Unknown host settings: {', '.join(sorted(unknown))}")
    defaults = HostConfig()
    port = payload.get("port", defaults.port)
    if isinstance(port, bool) or not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError("Host port must be an integer between 1024 and 65535.")
    address = payload.get("address", defaults.address)
    if not isinstance(address, str) or not address.strip() or "/" in address or any(character.isspace() for character in address):
        raise ValueError("Host address must be an IP address or hostname, without a URL prefix.")
    paths = {}
    for field in fields(HostConfig):
        if field.name in {"address", "port"}:
            continue
        value = payload.get(field.name, str(getattr(defaults, field.name)))
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Host setting {field.name} must be a nonempty file path.")
        location = Path(value)
        resolved = (location if location.is_absolute() else ROOT / location).resolve()
        if resolved.is_dir():
            raise ValueError(f"Host setting {field.name} must point to a file, not a folder.")
        paths[field.name] = resolved
    if len(set(paths.values())) != len(paths):
        raise ValueError("Each host data setting must point to a different file.")
    return HostConfig(address=address, port=port, **paths)


def data_revision(paths: tuple[Path, ...]) -> tuple[tuple[int, int], ...]:
    revision = []
    for path in paths:
        try:
            stat = path.stat()
            revision.append((stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            revision.append((0, 0))
    return tuple(revision)


def host_urls(address: str, port: int) -> list[str]:
    if address != "0.0.0.0":
        hostname = f"[{address}]" if ":" in address else address
        return [f"http://{hostname}:{port}"]
    try:
        addresses = sorted({item[4][0] for item in socket.getaddrinfo(socket.gethostname(), port, socket.AF_INET)})
    except OSError:
        addresses = []
    return [f"http://localhost:{port}", *[f"http://{ip}:{port}" for ip in addresses if not ip.startswith("127.")]]
