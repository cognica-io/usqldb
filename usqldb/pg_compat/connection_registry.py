#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""Module-level registry for active pgwire connections.

Used by :mod:`usqldb.pg_compat.pg_catalog` to populate
``pg_stat_activity`` with real connection data instead of
hardcoded placeholders.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


@dataclass
class ConnectionInfo:
    """Snapshot of a pgwire connection's current state."""

    pid: int = 0
    username: str = ""
    database: str = ""
    application_name: str = ""
    client_addr: str | None = None
    client_port: int = -1
    backend_start: datetime | None = None
    xact_start: datetime | None = None
    query_start: datetime | None = None
    state_change: datetime | None = None
    state: str = "idle"
    query: str = ""
    backend_type: str = "client backend"


_lock = threading.Lock()
_registry: dict[int, ConnectionInfo] = {}


def register(info: ConnectionInfo) -> None:
    """Add or update a connection in the registry."""
    with _lock:
        _registry[info.pid] = info


def unregister(pid: int) -> None:
    """Remove a connection from the registry."""
    with _lock:
        _registry.pop(pid, None)


def get_all() -> list[ConnectionInfo]:
    """Return a snapshot of all registered connections."""
    with _lock:
        return list(_registry.values())
