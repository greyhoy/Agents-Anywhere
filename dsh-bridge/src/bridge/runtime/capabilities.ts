import type { RuntimeStatus } from '../wire/protocol.js'
import type { Capability } from '../wire/protocol.js'

const RUNTIME_CAPABILITIES = [
  'runtime.config',
  'catalog.model',
  'catalog.permission',
  'catalog.effort',
  // Session operations the bridge implements regardless of Session state.
  // The connector derives startTurn/interruptTurn/commands from these ids
  // (dsh_capabilities in provider_config.py) and the server inherits them
  // for sessions without session-scoped capability rows (idle synced
  // sessions). Without this declaration DSH is permanently readOnly.
  'session.send_message',
  'session.interrupt',
  'session.steer',
  'session.commands',
  'session.interaction.approval',
] as const

const SESSION_CAPABILITIES = [
  'session.send_message',
  'session.interrupt',
  'session.steer',
  'session.interaction.approval',
  'session.commands',
] as const

/** Capabilities implemented by the process regardless of Session state. */
export function runtimeCapabilities(): Capability[] {
  return RUNTIME_CAPABILITIES.map(capabilityId => ({
    capabilityId,
    scope: 'runtime',
    supported: true,
    available: true,
    allowed: true,
  }))
}

/** Effective Session capabilities for one projected state. */
export function sessionCapabilities(sessionId: string, status: RuntimeStatus, modelAvailable: boolean): Capability[] {
  return SESSION_CAPABILITIES.map((capabilityId): Capability => {
    let available = status !== 'error'
    if (capabilityId === 'session.send_message') available = status === 'idle' && modelAvailable
    if (capabilityId === 'session.steer') available = status === 'running'
    if (capabilityId === 'session.interrupt') available = ['running', 'waiting_approval', 'blocked', 'stopping'].includes(status)
    if (capabilityId === 'session.commands') available = status === 'idle'
    if (capabilityId === 'session.interaction.approval') {
      available = status === 'waiting_approval' || status === 'blocked' || status === 'running' || status === 'idle'
    }
    return {
      capabilityId,
      scope: 'session',
      sessionId,
      supported: true,
      available,
      allowed: true,
      ...(available ? {} : { unavailableReason: unavailableReason(status, modelAvailable) }),
    }
  })
}

function unavailableReason(status: RuntimeStatus, modelAvailable: boolean): string {
  if (!modelAvailable) return 'Select a model that is available in the current DSH catalog.'
  return `The operation is unavailable while the Session status is ${status}.`
}
