"""Session-scoped capability notifications from the DSH bridge must reach the host.

The bridge emits ``session.capabilities.update`` (plural, direct notification)
and the sync stream emits ``session.capability.updated`` (singular). The
connector must forward both to ``host.session_capabilities_update``; otherwise
the server keeps projecting DSH sessions from protocol-level facts where
``session.send_message`` is unsupported, and clients disable the composer after
the first turn.
"""

from __future__ import annotations

import asyncio

import pytest

from connector.runtimes.dsh.runtime import DshRuntime


class _Host:
    connector_id = "connector-test"
    session_namespace = "connector-test"

    def __init__(self) -> None:
        self.session_capability_updates: list = []
        self.runtime_capability_updates: list = []

    async def runtime_capabilities_update(self, capabilities) -> None:
        self.runtime_capability_updates.append(capabilities)

    async def session_capabilities_update(self, capabilities) -> None:
        self.session_capability_updates.append(capabilities)


def _runtime() -> DshRuntime:
    host = _Host()
    runtime = DshRuntime.__new__(DshRuntime)
    runtime.host = host
    runtime._sync = None
    runtime._sync_mode = "polling"
    return runtime


_PARAMS = {
    "sessionId": "sess_test",
    "externalSessionId": "aa-test",
    "runtime": "dsh",
    "revision": 3,
    "capabilities": [
        {
            "capabilityId": "session.send_message",
            "scope": "session",
            "sessionId": "sess_test",
            "supported": True,
            "available": True,
            "allowed": True,
        }
    ],
}


@pytest.mark.parametrize(
    "method",
    ["session.capabilities.update", "session.capability.updated"],
)
def test_session_capability_notifications_reach_host(method: str) -> None:
    async def run() -> None:
        runtime = _runtime()
        await runtime._handle_notification(method, _PARAMS)
        assert len(runtime.host.session_capability_updates) == 1
        capability_set = runtime.host.session_capability_updates[0]
        send_message = next(
            capability
            for capability in capability_set.capabilities
            if capability.capability_id == "session.send_message"
        )
        assert send_message.supported is True
        assert send_message.available is True
        assert send_message.session_id == "sess_test"

    asyncio.run(run())


def test_runtime_capabilities_update_still_routed() -> None:
    async def run() -> None:
        runtime = _runtime()
        params = {
            "runtime": "dsh",
            "revision": 1,
            "capabilities": [
                {
                    "capabilityId": "runtime.config",
                    "scope": "runtime",
                    "supported": True,
                    "available": True,
                    "allowed": True,
                }
            ],
        }
        await runtime._handle_notification("runtime.capabilities.update", params)
        assert len(runtime.host.runtime_capability_updates) == 1
        assert runtime.host.session_capability_updates == []

    asyncio.run(run())
