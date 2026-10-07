"""AcpRuntime: bridges one ACP agent subprocess onto AgentRuntime."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from connector.runtime_protocol import (
    CAPABILITY_CATALOG_MODEL,
    CAPABILITY_SESSION_INTERACTION_APPROVAL,
    CAPABILITY_SESSION_INTERRUPT,
    CAPABILITY_SESSION_SEND_MESSAGE,
    RuntimeAttachment,
    RuntimeCapability,
    RuntimeCapabilitySet,
    RuntimeConfig,
    RuntimeIdentity,
    RuntimeOperationResult,
    RuntimeSessionStateCache,
    RuntimeTimelineSnapshot,
    SessionMeta,
    SessionNotice,
    SessionState,
)
from connector.runtime_protocol.protocol import AgentRuntime
from connector.runtime_protocol.host import RuntimeHostClient
from connector.runtime_protocol.models import RuntimeTimelineItem
from connector.runtime_protocol.timeline import (
    MarkdownMessageContent,
    MessageTimelineItem,
    ReasoningSystemContent,
    SystemTimelineItem,
    TimelineSource,
    ToolCallContent,
    ToolTimelineItem,
    timeline_content_hash,
)
from connector.runtimes.acp.client import AcpClient

ACP_RUNTIME = "acp"
_SYNC_STATE_KEY = "acp.sessions"


@dataclass(slots=True)
class AcpSession:
    session_id: str
    external_session_id: str
    cwd: str | None = None
    title: str | None = None
    turn_seq: int = 0
    active_turn_id: str | None = None
    order_seq: int = 0
    items: dict[str, RuntimeTimelineItem] = field(default_factory=dict)
    message_buffer: dict[str, list[str]] = field(default_factory=dict)
    thought_buffer: dict[str, list[str]] = field(default_factory=dict)


class AcpRuntime(AgentRuntime):
    """AgentRuntime implementation backed by an ACP agent subprocess."""

    def __init__(self, config: RuntimeConfig, host: RuntimeHostClient) -> None:
        self.config = config
        self.host = host
        values = dict(config.values)
        self.command: list[str] = list(values.get("command") or ["hermes", "acp"])
        self.cwd: str = str(values.get("cwd") or "/home/zjx")
        self.display_name: str = str(values.get("displayName") or "ACP Agent")
        self._sessions: dict[str, AcpSession] = {}
        self._sessions_by_external: dict[str, str] = {}
        self._session_states = RuntimeSessionStateCache(ACP_RUNTIME, self.host)
        self._client: AcpClient | None = None
        self._permission_futures: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._notices: dict[str, SessionNotice] = {}
        self._agent_version = "acp-0"
        self._agent_name = "acp-agent"
        self._lock = asyncio.Lock()
        self._stopping = False

    # ------------------------------------------------------------------ util

    def _next_order(self, session: AcpSession) -> int:
        session.order_seq += 1
        return session.order_seq

    def _stable_id(self, prefix: str, *parts: str) -> str:
        digest = hashlib.sha256("\x00".join(parts).encode()).hexdigest()[:16]
        return f"acp_{prefix}_{digest}"

    async def _publish_item(self, session: AcpSession, item: RuntimeTimelineItem) -> None:
        session.items[item.id] = item
        await self.host.timeline_item_upsert(item)

    async def _persist_sessions(self) -> None:
        payload: dict[str, Any] = {
            sid: {
                "externalSessionId": s.external_session_id,
                "cwd": s.cwd,
                "title": s.title,
                "turnSeq": s.turn_seq,
                "orderSeq": s.order_seq,
            }
            for sid, s in self._sessions.items()
        }
        await self.host.sync_state_write(_SYNC_STATE_KEY, payload)

    async def _restore_sessions(self) -> None:
        data = await self.host.sync_state_read(_SYNC_STATE_KEY)
        if not isinstance(data, dict):
            return
        for sid, entry in data.items():
            if not isinstance(entry, dict) or sid in self._sessions:
                continue
            external = str(entry.get("externalSessionId") or "")
            if not external:
                continue
            session = AcpSession(
                session_id=sid,
                external_session_id=external,
                cwd=entry.get("cwd"),
                title=entry.get("title"),
                turn_seq=int(entry.get("turnSeq") or 0),
                order_seq=int(entry.get("orderSeq") or 0),
            )
            self._sessions[sid] = session
            self._sessions_by_external[external] = sid
            await self.host.session_meta_upsert(
                session_id=sid,
                runtime=ACP_RUNTIME,
                external_session_id=external,
                title=session.title,
                cwd=session.cwd,
                ordering_time=None,
                metadata={"source": "acp.restore"},
            )
            await self._session_states.update(
                sid,
                external,
                "idle",
                metadata={"source": "acp.restore"},
            )

    # ------------------------------------------------------------- lifecycle

    @property
    def identity(self) -> RuntimeIdentity:
        return RuntimeIdentity(
            runtime=ACP_RUNTIME,
            runtime_version=self._agent_version,
            display_name=self.display_name,
        )

    async def start(self) -> None:
        client = AcpClient(
            command=self.command,
            cwd=self.cwd,
            notification_handler=self._on_notification,
            request_handler=self._on_request,
        )
        result = await client.start()
        self._client = client
        info = result.get("agentInfo") or {}
        if isinstance(info.get("name"), str):
            self._agent_name = info["name"]
        if isinstance(info.get("version"), str):
            self._agent_version = info["version"]
        await self._restore_sessions()
        await self.host.runtime_capabilities_update(self._runtime_capabilities())
        logger.info("ACP runtime started agent={} version={}", self._agent_name, self._agent_version)

    async def stop(self) -> None:
        self._stopping = True
        for future in tuple(self._permission_futures.values()):
            if not future.done():
                future.set_result({"outcome": {"outcome": "rejected"}})
        self._permission_futures.clear()
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def _ensure_client(self) -> AcpClient:
        if self._client is None or not self._client.connected:
            if self._stopping:
                raise RuntimeError("ACP runtime is stopping")
            await self.start()
        client = self._client
        assert client is not None
        return client

    # ---------------------------------------------------------- capabilities

    def _runtime_capabilities(self) -> RuntimeCapabilitySet:
        def cap(capability_id: str, supported: bool = True) -> RuntimeCapability:
            return RuntimeCapability(
                capability_id=capability_id,
                scope="runtime",
                runtime=ACP_RUNTIME,
                connector_id=self.host.connector_id,
                supported=supported,
                available=supported,
                allowed=True,
                metadata={"source": "acp.runtime"},
            )

        return RuntimeCapabilitySet(
            runtime=ACP_RUNTIME,
            revision=self.config.revision,
            connector_id=self.host.connector_id,
            capabilities=(
                cap(CAPABILITY_SESSION_SEND_MESSAGE),
                cap(CAPABILITY_SESSION_INTERRUPT),
                cap(CAPABILITY_SESSION_INTERACTION_APPROVAL),
                cap(CAPABILITY_CATALOG_MODEL, False),
            ),
            metadata={"source": "acp.runtime"},
        )

    async def get_runtime_capabilities(self) -> RuntimeCapabilitySet:
        return self._runtime_capabilities()

    async def get_session_capabilities(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> RuntimeCapabilitySet:
        active = self._sessions.get(session_id) is not None and (
            self._sessions[session_id].active_turn_id is not None
        )
        return RuntimeCapabilitySet(
            runtime=ACP_RUNTIME,
            revision=self.config.revision,
            session_id=session_id,
            connector_id=self.host.connector_id,
            capabilities=(
                RuntimeCapability(
                    capability_id=CAPABILITY_SESSION_SEND_MESSAGE,
                    scope="session",
                    runtime=ACP_RUNTIME,
                    session_id=session_id,
                    connector_id=self.host.connector_id,
                    supported=True,
                    available=not active,
                    unavailable_reason="turn_active" if active else None,
                    metadata={"source": "acp.runtime"},
                ),
                RuntimeCapability(
                    capability_id=CAPABILITY_SESSION_INTERRUPT,
                    scope="session",
                    runtime=ACP_RUNTIME,
                    session_id=session_id,
                    connector_id=self.host.connector_id,
                    supported=True,
                    available=active,
                    unavailable_reason=None if active else "no_active_turn",
                    metadata={"source": "acp.runtime"},
                ),
            ),
            metadata={"source": "acp.runtime"},
        )

    async def get_config(self) -> RuntimeConfig:
        return self.config

    # -------------------------------------------------------------- sessions

    def _session(self, session_id: str) -> AcpSession | None:
        return self._sessions.get(session_id)

    def _resolve_session(
        self, session_id: str, external_session_id: str | None
    ) -> AcpSession | None:
        session = self._session(session_id)
        if session is not None:
            return session
        if external_session_id is not None:
            mapped = self._sessions_by_external.get(external_session_id)
            if mapped is not None:
                return self._sessions.get(mapped)
        return None

    async def list_sessions(
        self,
        limit: int = 100,
        cursor: str | None = None,
        force: bool = False,
    ) -> tuple[SessionMeta, ...]:
        metas = []
        for session in self._sessions.values():
            metas.append(
                SessionMeta(
                    session_id=session.session_id,
                    external_session_id=session.external_session_id,
                    runtime=ACP_RUNTIME,
                    title=session.title,
                    cwd=session.cwd,
                    ordering_time=None,
                    metadata={"source": "acp.runtime"},
                )
            )
        return tuple(metas[:limit])

    async def get_session_snapshot(
        self,
        session_id: str,
        external_session_id: str | None = None,
        limit: int | None = None,
    ) -> RuntimeTimelineSnapshot:
        session = self._resolve_session(session_id, external_session_id)
        if session is None:
            return RuntimeTimelineSnapshot(
                session_id=session_id,
                external_session_id=external_session_id,
                runtime=ACP_RUNTIME,
                items=(),
                complete=True,
                metadata={"source": "acp.runtime", "reason": "unknown_session"},
            )
        items = tuple(
            sorted(session.items.values(), key=lambda item: item.order_seq)
        )
        if limit is not None:
            items = items[-limit:]
        return RuntimeTimelineSnapshot(
            session_id=session.session_id,
            external_session_id=session.external_session_id,
            runtime=ACP_RUNTIME,
            items=items,
            complete=True,
            metadata={"source": "acp.runtime"},
        )

    async def get_session_state(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> SessionState | None:
        return self._session_states.get(session_id)

    async def get_session_notices(
        self,
        session_id: str,
        external_session_id: str | None = None,
    ) -> tuple[SessionNotice, ...]:
        return tuple(
            notice
            for notice in self._notices.values()
            if notice.session_id == session_id and notice.status == "open"
        )

    # ----------------------------------------------------------------- turns

    async def create_and_start_session(
        self,
        session_id: str,
        content: str,
        title: str | None = None,
        cwd: str | None = None,
        selections: Mapping[str, str | None] | None = None,
        attachments: tuple[RuntimeAttachment, ...] = (),
        client_message_id: str | None = None,
        runtime_options: Mapping[str, Any] | None = None,
    ) -> RuntimeOperationResult:
        if self._session(session_id) is not None:
            return await self.start_turn(
                session_id=session_id,
                external_session_id=None,
                content=content,
                selections=selections,
                attachments=attachments,
                client_message_id=client_message_id,
                cwd=cwd,
            )
        client = await self._ensure_client()
        result = await client.request(
            "session/new",
            {"cwd": cwd or self.cwd, "mcpServers": []},
        )
        external = str((result or {}).get("sessionId") or "")
        if not external:
            return RuntimeOperationResult(
                ok=False,
                code="acp_session_new_failed",
                message="ACP agent did not return a session id",
            )
        session = AcpSession(
            session_id=session_id,
            external_session_id=external,
            cwd=cwd or self.cwd,
            title=title,
        )
        self._sessions[session_id] = session
        self._sessions_by_external[external] = session_id
        await self.host.session_meta_upsert(
            session_id=session_id,
            runtime=ACP_RUNTIME,
            external_session_id=external,
            title=title,
            cwd=session.cwd,
            ordering_time=None,
            metadata={"source": "acp.session.new"},
        )
        await self._persist_sessions()
        return await self._run_turn(
            session,
            content,
            client_message_id=client_message_id,
        )

    async def start_turn(
        self,
        session_id: str,
        external_session_id: str | None,
        content: str,
        selections: Mapping[str, str | None] | None = None,
        attachments: tuple[RuntimeAttachment, ...] = (),
        client_message_id: str | None = None,
        cwd: str | None = None,
    ) -> RuntimeOperationResult:
        session = self._resolve_session(session_id, external_session_id)
        if session is None:
            return RuntimeOperationResult(
                ok=False,
                code="acp_session_not_found",
                message="ACP session was not found",
            )
        if session.active_turn_id is not None:
            return RuntimeOperationResult(
                ok=False,
                code="acp_turn_active",
                message="A turn is already running on this session",
            )
        await self._ensure_client()
        return await self._run_turn(
            session,
            content,
            client_message_id=client_message_id,
        )

    async def interrupt_session(
        self,
        session_id: str,
        reason: str | None = None,
    ) -> RuntimeOperationResult:
        session = self._session(session_id)
        if session is None:
            return RuntimeOperationResult(
                ok=False,
                code="acp_session_not_found",
                message="ACP session was not found",
            )
        client = self._client
        if client is None or not client.connected:
            return RuntimeOperationResult(
                ok=False,
                code="acp_not_connected",
                message="ACP agent is not running",
            )
        try:
            await client.request(
                "session/cancel",
                {"sessionId": session.external_session_id},
                timeout=30,
            )
        except Exception as exc:
            return RuntimeOperationResult(
                ok=False,
                code="acp_cancel_failed",
                message=f"Failed to cancel ACP turn: {exc}",
            )
        return RuntimeOperationResult(ok=True)

    async def respond_interaction(
        self,
        session_id: str,
        notice_id: str,
        action_id: str,
        input_data: Mapping[str, Any] | None = None,
    ) -> RuntimeOperationResult:
        notice = self._notices.get(notice_id)
        if notice is None or notice.session_id != session_id:
            return RuntimeOperationResult(
                ok=False,
                code="acp_notice_not_found",
                message="ACP permission notice was not found",
            )
        future = self._permission_futures.get(notice_id)
        if future is None or future.done():
            return RuntimeOperationResult(
                ok=False,
                code="acp_notice_not_pending",
                message="ACP permission notice is not waiting for a response",
            )
        if action_id in {"approve", "approved", "accept", "allow"}:
            outcome = {"outcome": {"outcome": "selected", "optionId": "allow"}}
        elif input_data and isinstance(input_data.get("optionId"), str):
            outcome = {
                "outcome": {
                    "outcome": "selected",
                    "optionId": input_data["optionId"],
                }
            }
        else:
            outcome = {"outcome": {"outcome": "rejected"}}
        future.set_result(outcome)
        return RuntimeOperationResult(ok=True)

    async def _run_turn(
        self,
        session: AcpSession,
        content: str,
        client_message_id: str | None = None,
    ) -> RuntimeOperationResult:
        session.turn_seq += 1
        turn_id = f"acp-turn-{session.turn_seq}"
        session.active_turn_id = turn_id

        user_item_id = self._stable_id(
            "msg", session.session_id, turn_id, "user", content
        )
        await self._publish_item(
            session,
            MessageTimelineItem(
                id=user_item_id,
                type="message",
                status="done",
                role="user",
                turn_id=turn_id,
                content=MarkdownMessageContent(text=content),
                source=TimelineSource(
                    runtime=ACP_RUNTIME,
                    external_session_id=session.external_session_id,
                    turn_id=turn_id,
                    event="acp.user_message",
                    client_message_id=client_message_id,
                ),
            ).to_platform_item(
                session_id=session.session_id, order_seq=self._next_order(session)
            ),
        )

        await self._session_states.update(
            session.session_id,
            session.external_session_id,
            "running",
            metadata={"source": "acp.turn.running"},
        )

        client = self._client
        assert client is not None
        asyncio.create_task(
            self._drive_turn(session, turn_id, content, client)
        )
        return RuntimeOperationResult(ok=True, result={"turnId": turn_id})

    async def _drive_turn(
        self,
        session: AcpSession,
        turn_id: str,
        content: str,
        client: AcpClient,
    ) -> None:
        outcome = "completed"
        error_payload: Mapping[str, Any] | None = None
        try:
            result = await client.request(
                "session/prompt",
                {
                    "sessionId": session.external_session_id,
                    "prompt": [{"type": "text", "text": content}],
                },
                timeout=1800,
            )
            stop_reason = str((result or {}).get("stopReason") or "end_turn")
            if stop_reason not in {"end_turn", "cancelled"}:
                outcome = "failed"
                error_payload = {
                    "code": "acp_turn_failed",
                    "message": f"ACP turn stopped: {stop_reason}",
                }
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            outcome = "failed"
            error_payload = {
                "code": "acp_turn_failed",
                "message": str(exc)[:500],
            }
        finally:
            await self._finalize_turn_items(session, turn_id, outcome)
            session.active_turn_id = None
            await self.host.session_turn_ended(
                session_id=session.session_id,
                runtime=ACP_RUNTIME,
                external_session_id=session.external_session_id,
                turn_id=turn_id,
                outcome=outcome,
                metadata={"source": f"acp.turn.{outcome}"},
            )
            await self._session_states.update(
                session.session_id,
                session.external_session_id,
                "error" if outcome == "failed" else "idle",
                error=error_payload,
                metadata={"source": f"acp.turn.{outcome}"},
            )
            await self._persist_sessions()

    # ------------------------------------------------------- ACP event sink

    async def _finalize_turn_items(
        self, session: AcpSession, turn_id: str, outcome: str
    ) -> None:
        """Mark streaming items (assistant message / reasoning / tools) done or failed."""
        final_status = "done" if outcome == "completed" else "failed"
        for item_id, existing in list(session.items.items()):
            if existing.turn_id != turn_id or existing.status in {"done", "failed", "cancelled"}:
                continue
            if existing.type not in {"message", "system", "tool"}:
                continue
            updated = RuntimeTimelineItem(
                id=existing.id,
                session_id=existing.session_id,
                type=existing.type,
                status=final_status,
                order_seq=existing.order_seq,
                content_hash=timeline_content_hash(
                    item_type=existing.type,  # type: ignore[arg-type]
                    status=final_status,  # type: ignore[arg-type]
                    role=existing.role,  # type: ignore[arg-type]
                    content=existing.content,
                ),
                role=existing.role,
                turn_id=existing.turn_id,
                content=existing.content,
                source=existing.source,
                revision=existing.revision + 1,
                metadata=existing.metadata,
            )
            session.items[item_id] = updated
            try:
                await self.host.timeline_item_upsert(updated)
            except Exception:
                logger.exception("ACP finalize upsert failed item={}", item_id)

    async def _on_notification(self, method: str, params: dict[str, Any]) -> None:
        if method != "session/update":
            return
        session_id = params.get("sessionId")
        if not isinstance(session_id, str):
            return
        platform_id = self._sessions_by_external.get(session_id)
        if platform_id is None:
            return
        session = self._sessions.get(platform_id)
        if session is None:
            return
        update = params.get("update") or {}
        if not isinstance(update, dict):
            return
        kind = update.get("sessionUpdate")
        turn_id = session.active_turn_id or "acp-unknown-turn"
        try:
            await self._project_update(session, turn_id, kind, update)
        except Exception:
            logger.exception("ACP update projection failed kind={}", kind)

    async def _project_update(
        self,
        session: AcpSession,
        turn_id: str,
        kind: Any,
        update: dict[str, Any],
    ) -> None:
        if kind in {"agent_message_chunk", "agent_thought_chunk"}:
            content = update.get("content") or {}
            text = str(content.get("text") or "")
            if not text:
                return
            role = "assistant" if kind == "agent_message_chunk" else "system"
            buffer = (
                session.message_buffer
                if role == "assistant"
                else session.thought_buffer
            )
            buffer.setdefault(turn_id, []).append(text)
            joined = "".join(buffer[turn_id])
            if role == "assistant":
                item_id = self._stable_id(
                    "msg", session.session_id, turn_id, "assistant"
                )
                content_obj: Any = MarkdownMessageContent(text=joined)
                item_type = "message"
                status = "running"
            else:
                item_id = self._stable_id(
                    "reason", session.session_id, turn_id
                )
                content_obj = ReasoningSystemContent(text=joined)
                item_type = "system"
                status = "running"
            item = RuntimeTimelineItem(
                id=item_id,
                session_id=session.session_id,
                type=item_type,
                status=status,
                order_seq=self._next_order(session) if item_id not in session.items else session.items[item_id].order_seq,
                content_hash=timeline_content_hash(
                    item_type=item_type,
                    status=status,
                    role=role,
                    content=_content_mapping(content_obj),
                ),
                role=role,
                turn_id=turn_id,
                content=_content_mapping(content_obj),
                source=TimelineSource(
                    runtime=ACP_RUNTIME,
                    external_session_id=session.external_session_id,
                    turn_id=turn_id,
                    event=f"acp.{kind}",
                ).to_mapping(),
                revision=1,
            )
            session.items[item_id] = item
            await self.host.timeline_item_upsert(item)
            return

        if kind == "tool_call":
            tool_call_id = str(update.get("toolCallId") or "")
            title = str(update.get("title") or "tool")
            item_id = self._stable_id(
                "tool", session.session_id, turn_id, tool_call_id
            )
            item = ToolTimelineItem(
                id=item_id,
                type="tool",
                status="running",
                role="tool",
                turn_id=turn_id,
                content=ToolCallContent(
                    kind="tool_call",
                    title=title,
                    input=update.get("rawInput"),
                ),
                source=TimelineSource(
                    runtime=ACP_RUNTIME,
                    external_session_id=session.external_session_id,
                    turn_id=turn_id,
                    native_item_id=tool_call_id,
                    event="acp.tool_call",
                ),
            ).to_platform_item(
                session_id=session.session_id, order_seq=self._next_order(session)
            )
            session.items[item_id] = item
            await self.host.timeline_item_upsert(item)
            return

        if kind == "tool_call_update":
            tool_call_id = str(update.get("toolCallId") or "")
            item_id = self._stable_id(
                "tool", session.session_id, turn_id, tool_call_id
            )
            existing = session.items.get(item_id)
            if existing is None:
                return
            updated = RuntimeTimelineItem(
                id=existing.id,
                session_id=existing.session_id,
                type=existing.type,
                status="done",
                order_seq=existing.order_seq,
                content_hash=timeline_content_hash(
                    item_type=existing.type,  # type: ignore[arg-type]
                    status="done",  # type: ignore[arg-type]
                    role=existing.role,  # type: ignore[arg-type]
                    content={
                        **dict(existing.content),
                        "output": update.get("content"),
                    },
                ),
                role=existing.role,
                turn_id=existing.turn_id,
                content={
                    **dict(existing.content),
                    "output": update.get("content"),
                },
                source=existing.source,
                revision=existing.revision + 1,
                metadata=existing.metadata,
            )
            session.items[item_id] = updated
            await self.host.timeline_item_upsert(updated)
            return

        if kind in {"plan", "available_commands_update", "usage_update"}:
            # informational; not projected onto the timeline for now.
            return

        logger.debug("ACP update ignored kind={}", kind)

    # ------------------------------------------------- ACP agent -> client

    async def _on_request(
        self, method: str, params: dict[str, Any], request_id: Any
    ) -> Any:
        if method == "session/request_permission":
            return await self._handle_permission_request(params)
        if method in {"fs/read_text_file", "fs/write_text_file"}:
            raise PermissionError(
                "Connector does not expose the local filesystem to ACP agents"
            )
        raise RuntimeError(f"Unsupported ACP request: {method}")

    async def _handle_permission_request(self, params: dict[str, Any]) -> Any:
        session_id = params.get("sessionId")
        if not isinstance(session_id, str):
            raise ValueError("permission request without sessionId")
        platform_id = self._sessions_by_external.get(session_id)
        if platform_id is None:
            return {"outcome": {"outcome": "rejected"}}
        options = params.get("options") or []
        actions = [
            {
                "actionId": str(option.get("optionId") or "allow"),
                "label": str(option.get("name") or option.get("optionId") or "Allow"),
                "style": "primary",
            }
            for option in options
            if isinstance(option, dict)
        ]
        if not actions:
            actions = [{"actionId": "allow", "label": "Allow", "style": "primary"}]
        actions.append(
            {"actionId": "reject", "label": "Reject", "style": "danger"}
        )
        title = "ACP agent requests permission"
        descriptions = [
            str(option.get("kind") or "")
            for option in options
            if isinstance(option, dict)
        ]
        message = ", ".join(part for part in descriptions if part) or "Tool use"
        notice_id = self._stable_id(
            "perm", platform_id, str(params.get("requestId") or str(time.time()))
        )
        notice = SessionNotice(
            notice_id=notice_id,
            session_id=platform_id,
            runtime=ACP_RUNTIME,
            type="interaction",
            title=title,
            message=message[:500],
            severity="warning",
            status="open",
            interaction_type="approval",
            blocking={"scope": "session", "targetId": platform_id},
            response_required=True,
            actions=tuple(actions),
            source={"runtime": ACP_RUNTIME, "event": "acp.request_permission"},
        )
        self._notices[notice_id] = notice
        await self.host.notice_upsert(notice)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._permission_futures[notice_id] = future
        try:
            return await asyncio.wait_for(future, 3600)
        except asyncio.TimeoutError:
            return {"outcome": {"outcome": "rejected"}}
        finally:
            self._permission_futures.pop(notice_id, None)
            resolved = SessionNotice(
                notice_id=notice_id,
                session_id=platform_id,
                runtime=ACP_RUNTIME,
                type="interaction",
                title=title,
                message=notice.message,
                severity="warning",
                status="resolved",
                interaction_type="approval",
                response_required=False,
                actions=(),
                source=notice.source,
                context={"approvalStatus": "resolved"},
            )
            self._notices[notice_id] = resolved
            await self.host.notice_upsert(resolved)


def _content_mapping(content: Any) -> dict[str, Any]:
    mapping = content.to_mapping()
    return dict(mapping)
