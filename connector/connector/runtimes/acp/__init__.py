"""ACP runtime adapter for Agents Anywhere.

Spawns an ACP (Agent Client Protocol) agent subprocess (e.g. `hermes acp`,
`openclaw acp`) and bridges it onto the AgentRuntime contract:

  initialize            -> runtime start
  session/new           -> create_and_start_session
  session/load          -> session recovery after restart
  session/prompt        -> start_turn (streaming session/update -> timeline)
  session/cancel        -> interrupt_session
  session/list          -> list_sessions
  session/request_permission -> SessionNotice interaction (approval UI)

Wire protocol facts verified against `hermes acp` 0.18.2 on 2026-10-07:
- JSON-RPC 2.0 over stdio, newline-delimited frames.
- session/new REQUIRES `mcpServers` (list) in params.
- session/list REQUIRES params to be an object (e.g. {}).
- Streaming updates use `session/update` with
  `update.sessionUpdate` (not `updateType`) as the discriminator.
- prompt result: {"stopReason": "end_turn", "usage": {...}}.
"""

from connector.runtimes.acp.provider import AcpProvider

__all__ = ["AcpProvider"]
