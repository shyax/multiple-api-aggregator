"""Core data types passing through the aggregator."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class Priority(IntEnum):
    """Lower integer = higher scheduling priority (min-heap semantics)."""

    HIGH = 0
    MEDIUM = 50
    LOW = 100


@dataclass
class Request:
    api: str
    method: str = "GET"
    url: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    headers: dict[str, str] = field(default_factory=dict)
    priority: Priority = Priority.MEDIUM
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 10.0
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    created_at: float = field(default_factory=time.monotonic)
    enqueued_at: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def dedup_key(self) -> str:
        """Stable key used by the dedup layer to coalesce identical in-flight calls."""
        param_repr = repr(sorted(self.params.items())) if self.params else ""
        body_repr = repr(self.body) if self.body else ""
        return f"{self.api}|{self.method}|{self.url}|{param_repr}|{body_repr}"


@dataclass
class Response:
    request_id: str
    api: str
    status: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    latency: float = 0.0
    attempts: int = 1
    completed_at: float = field(default_factory=time.monotonic)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300
