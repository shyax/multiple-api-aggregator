"""Adapter interface — wraps a per-API client.

Two flavours ship out of the box:

- `HTTPAdapter`: aiohttp-backed, suitable for real upstreams.
- `CallableAdapter`: wraps an async callable. Used by mock APIs and tests so
  the rest of the system can be exercised without touching the network.
"""

from __future__ import annotations

import time
from typing import Awaitable, Callable, Protocol

from aggregator.common.exceptions import (
    NonRetryableError,
    RateLimitedError,
    UpstreamError,
)
from aggregator.common.types import Request, Response


class BaseAdapter(Protocol):
    name: str

    async def execute(self, request: Request) -> Response: ...

    async def aclose(self) -> None: ...


class CallableAdapter:
    """Adapter that delegates to a user-supplied async callable.

    The callable receives a `Request` and must return a tuple
    `(status, body, headers)`. This indirection keeps the adapter contract
    decoupled from any specific HTTP library.
    """

    def __init__(
        self,
        name: str,
        handler: Callable[[Request], Awaitable[tuple[int, object, dict[str, str]]]],
    ) -> None:
        self.name = name
        self._handler = handler

    async def execute(self, request: Request) -> Response:
        start = time.monotonic()
        try:
            status, body, headers = await self._handler(request)
        except (RateLimitedError, UpstreamError, NonRetryableError):
            raise
        except (TimeoutError, ConnectionError) as exc:
            raise UpstreamError(str(exc)) from exc
        latency = time.monotonic() - start
        if status == 429:
            retry_after = float(headers.get("Retry-After", 0)) or None
            raise RateLimitedError("upstream 429", retry_after=retry_after)
        if 500 <= status < 600:
            raise UpstreamError(f"upstream {status}")
        if 400 <= status < 500:
            raise NonRetryableError(f"client error {status}")
        return Response(
            request_id=request.id,
            api=request.api,
            status=status,
            body=body,
            headers=headers,
            latency=latency,
            attempts=request.retry_count + 1,
        )

    async def aclose(self) -> None:
        return None


class HTTPAdapter:
    """aiohttp-based adapter. Lazy session so adapters created in tests don't
    require a running event loop.
    """

    def __init__(self, name: str, base_url: str = "", timeout: float = 10.0) -> None:
        self.name = name
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = None  # type: ignore[assignment]

    async def _ensure_session(self):
        if self._session is None:
            import aiohttp  # local import: keep adapters/base importable without aiohttp installed

            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout)
            )
        return self._session

    async def execute(self, request: Request) -> Response:
        session = await self._ensure_session()
        url = request.url if request.url.startswith("http") else f"{self._base_url}{request.url}"
        start = time.monotonic()
        try:
            async with session.request(
                request.method,
                url,
                params=request.params or None,
                json=request.body,
                headers=request.headers or None,
                timeout=request.timeout,
            ) as resp:
                body = await resp.text()
                latency = time.monotonic() - start
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    raise RateLimitedError(
                        "upstream 429",
                        retry_after=float(retry_after) if retry_after else None,
                    )
                if 500 <= resp.status < 600:
                    raise UpstreamError(f"upstream {resp.status}")
                if 400 <= resp.status < 500:
                    raise NonRetryableError(f"client error {resp.status}")
                return Response(
                    request_id=request.id,
                    api=request.api,
                    status=resp.status,
                    body=body,
                    headers=dict(resp.headers),
                    latency=latency,
                    attempts=request.retry_count + 1,
                )
        except (TimeoutError, ConnectionError) as exc:
            raise UpstreamError(str(exc)) from exc

    async def aclose(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
