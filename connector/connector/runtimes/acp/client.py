"""ACP subprocess client: JSON-RPC 2.0 over stdio (newline-delimited)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

MAX_FRAME_BYTES = 16 * 1024 * 1024
NotificationHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


class AcpRpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"ACP error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class AcpClient:
    """Manages one ACP agent subprocess and its JSON-RPC framing."""

    def __init__(
        self,
        *,
        command: list[str],
        cwd: str,
        env: dict[str, str] | None = None,
        notification_handler: NotificationHandler,
        request_handler: Callable[[str, dict[str, Any], Any], Awaitable[Any]] | None = None,
        startup_timeout: float = 120.0,
        request_timeout: float = 600.0,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.env = env
        self.notification_handler = notification_handler
        self.request_handler = request_handler
        self.startup_timeout = startup_timeout
        self.request_timeout = request_timeout
        self.proc: asyncio.subprocess.Process | None = None
        self.initialize_result: dict[str, Any] | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._pending_requests: dict[int, dict[str, Any]] = {}
        self._next_id = 1
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._closing = False
        self._early_notifications: list[tuple[str, dict[str, Any]]] = []

    @property
    def connected(self) -> bool:
        return (
            not self._closing
            and self.proc is not None
            and self.proc.returncode is None
            and self._reader_task is not None
            and not self._reader_task.done()
        )

    async def start(self) -> dict[str, Any]:
        """Spawn the agent and run the ACP initialize handshake."""
        if self.proc is not None:
            raise RuntimeError("ACP client is already started")
        self._closing = False
        self.proc = await asyncio.create_subprocess_exec(
            *self.command,
            cwd=self.cwd,
            env=self.env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            limit=MAX_FRAME_BYTES + 1,
        )
        self._reader_task = asyncio.create_task(
            self._read_frames(), name="acp-stdio-reader"
        )
        try:
            result = await self.request(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": False, "writeTextFile": False},
                    },
                },
                timeout=self.startup_timeout,
            )
        except BaseException:
            await self.close()
            raise
        if not isinstance(result, dict):
            await self.close()
            raise RuntimeError("ACP initialize result must be an object")
        self.initialize_result = result
        for method, params in self._early_notifications:
            await self._dispatch_notification(method, params)
        self._early_notifications.clear()
        logger.info(
            "ACP agent started command={} agent={} version={}",
            " ".join(self.command[:2]),
            (result.get("agentInfo") or {}).get("name"),
            (result.get("agentInfo") or {}).get("version"),
        )
        return result

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        request_id = self._next_id
        self._next_id += 1
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            payload["params"] = params
        await self._send(payload)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future
        effective_timeout = timeout or self.request_timeout
        try:
            return await asyncio.wait_for(future, effective_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise TimeoutError(
                f"ACP request {method!r} timed out after {effective_timeout}s"
            ) from None
        finally:
            self._pending.pop(request_id, None)

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        await self._send(payload)

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        proc = self.proc
        if proc is not None and proc.returncode is None and proc.stdin is not None:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), 10)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(RuntimeError("ACP connection closed"))
        self._pending.clear()
        for request in tuple(self._pending_requests.values()):
            await self._reply_unsupported(request)
        self._pending_requests.clear()

    async def _send(self, payload: dict[str, Any]) -> None:
        proc = self.proc
        if proc is None or proc.stdin is None or proc.returncode is not None:
            raise RuntimeError("ACP agent process is not running")
        encoded = (
            json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
            + b"\n"
        )
        if len(encoded) > MAX_FRAME_BYTES:
            raise ValueError("ACP request frame exceeds 16 MiB")
        async with self._write_lock:
            if proc.stdin is None:
                raise RuntimeError("ACP agent stdin closed")
            proc.stdin.write(encoded)
            await proc.stdin.drain()

    async def _read_frames(self) -> None:
        proc = self.proc
        assert proc is not None and proc.stdout is not None
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                if len(line) > MAX_FRAME_BYTES:
                    raise RuntimeError("ACP response frame exceeds 16 MiB")
                self._handle_frame(line)
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            logger.error("ACP protocol failure error_type={}", exc.__class__.__name__)
        finally:
            for future in tuple(self._pending.values()):
                if not future.done():
                    future.set_exception(RuntimeError("ACP agent connection closed"))
            self._pending.clear()
            if not self._closing:
                logger.warning("ACP agent process exited unexpectedly")

    def _handle_frame(self, line: bytes) -> None:
        value = json.loads(line.decode("utf-8"))
        if not isinstance(value, dict) or value.get("jsonrpc") != "2.0":
            raise RuntimeError("ACP agent emitted an invalid JSON-RPC frame")
        if "id" in value and "method" not in value:
            request_id = value.get("id")
            future = self._pending.get(request_id) if request_id is not None else None
            if future is None or future.done():
                return
            error = value.get("error")
            if error is not None:
                future.set_exception(
                    AcpRpcError(
                        error.get("code", -32000),
                        str(error.get("message") or "ACP request failed"),
                        error.get("data"),
                    )
                )
                return
            future.set_result(value.get("result"))
            return
        method = value.get("method")
        params = value.get("params") or {}
        if not isinstance(method, str) or not isinstance(params, dict):
            raise RuntimeError("ACP agent emitted an invalid request or notification")
        if "id" in value and self.request_handler is not None:
            self._pending_requests[value["id"]] = value
            asyncio.get_running_loop().create_task(
                self._serve_request(value)
            )
            return
        if "id" in value:
            asyncio.get_running_loop().create_task(
                self._reply_unsupported(value)
            )
            return
        if self.initialize_result is None:
            self._early_notifications.append((method, params))
            return
        asyncio.get_running_loop().create_task(
            self._dispatch_notification(method, params)
        )

    async def _serve_request(self, value: dict[str, Any]) -> None:
        request_id = value.get("id")
        method = value.get("method")
        if not isinstance(method, str) or request_id is None:
            return
        params = value.get("params") or {}
        try:
            if self.request_handler is None:
                raise RuntimeUnsupported("request handler")
            result = await self.request_handler(method, params, request_id)
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "result": result}
            )
        except Exception as exc:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32603,
                        "message": f"connector failed to handle {method}: {exc}",
                    },
                }
            )

    async def _reply_unsupported(self, value: dict[str, Any]) -> None:
        request_id = value.get("id")
        if request_id is None:
            return
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": "Connector does not accept this ACP request",
                    },
                }
            )
        except Exception:
            pass

    async def _dispatch_notification(self, method: str, params: dict[str, Any]) -> None:
        try:
            await self.notification_handler(method, params)
        except Exception:
            logger.exception("ACP notification handler failed method={}", method)


class RuntimeUnsupported(Exception):
    pass
