import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type { Agent, AgentHandle, ModelSelection } from '@deepseek-ai/dsh-agent'
import { SessionId, type SessionEvent, type SessionHeader } from '@deepseek-ai/dsh-session'
import {
  SessionPersistenceRevision,
  type SessionPersistenceSnapshot,
} from '@deepseek-ai/dsh-session-persistence'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { MetadataStore } from '../src/bridge/persistence/metadata.js'
import { modelSelectionId, permissionSelectionId } from '../src/bridge/projection/identity.js'
import type { CatalogManager, CatalogSnapshot } from '../src/bridge/runtime/catalogs.js'
import { SessionManager } from '../src/bridge/runtime/sessions.js'

const roots: string[] = []
const originalModel: ModelSelection = { provider: 'test', model: 'alpha' }

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })))
})

function turnStart(seq = 0): SessionEvent {
  return { type: 'turn/start', seq, time: seq + 2, data: { turn: 1 } }
}

function userMessage(seq = 0): SessionEvent {
  return {
    type: 'user/message',
    seq,
    time: seq + 2,
    data: {
      id: `message-${seq}` as never,
      role: 'user',
      content: [{ type: 'text', text: 'hello' }],
      source: { kind: 'user' },
    },
  } as SessionEvent
}

describe('archived Session write gate', () => {
  it('auto-unarchives a DSH-archived Session on startTurn and republishes visibility', async () => {
    const fixture = await archivedFixture({ unarchiveSupported: true })
    const receipt = await fixture.manager.startTurn({
      sessionId: fixture.platformSessionId,
      externalSessionId: String(fixture.sessionId),
      content: 'resume after archive',
      clientMessageId: 'archived-turn-1',
    })
    expect(receipt).toMatchObject({
      ok: true,
      result: { sessionId: fixture.platformSessionId, externalSessionId: String(fixture.sessionId) },
    })
    expect(fixture.unarchive).toHaveBeenCalledTimes(1)
    expect(fixture.unarchive).toHaveBeenCalledWith(fixture.sessionId)
    const meta = fixture.emitted.find(item => item.method === 'session.meta.upsert')
    expect(meta).toBeDefined()
    expect((meta?.params as Record<string, unknown>).metadata).toMatchObject({ localArchived: false, hidden: false })
    expect(fixture.followup).toHaveBeenCalledTimes(1)
  })

  it('rejects startTurn with SESSION_ARCHIVED on hosts without unarchiveSession', async () => {
    const fixture = await archivedFixture({ unarchiveSupported: false })
    await expect(fixture.manager.startTurn({
      sessionId: fixture.platformSessionId,
      externalSessionId: String(fixture.sessionId),
      content: 'resume after archive',
      clientMessageId: 'archived-turn-2',
    })).rejects.toMatchObject({
      data: {
        code: 'SESSION_ARCHIVED',
        retryable: false,
        externalSessionId: String(fixture.sessionId),
      },
    })
    expect(fixture.followup).not.toHaveBeenCalled()
  })

  it('leaves unarchived Sessions untouched (no registry writes, no extra meta events)', async () => {
    const fixture = await archivedFixture({ unarchiveSupported: true, archived: false })
    await fixture.manager.startTurn({
      sessionId: fixture.platformSessionId,
      externalSessionId: String(fixture.sessionId),
      content: 'ordinary turn',
      clientMessageId: 'plain-turn-1',
    })
    expect(fixture.unarchive).not.toHaveBeenCalled()
  })
})

interface ArchivedFixture {
  manager: SessionManager
  platformSessionId: string
  sessionId: SessionId
  unarchive: ReturnType<typeof vi.fn>
  followup: ReturnType<typeof vi.fn>
  emitted: { method: string, params: Record<string, unknown> }[]
}

async function archivedFixture(options: { unarchiveSupported: boolean, archived?: boolean }): Promise<ArchivedFixture> {
  const archived = options.archived ?? true
  const sessionId = SessionId('archived-gate')
  const platformSessionId = 'platform-archived-gate'
  const header: SessionHeader = { version: 0, id: sessionId, createdAt: 1, cwd: '/workspace' }
  const events: SessionEvent[] = [turnStart(), userMessage(1)]
  const snapshot: SessionPersistenceSnapshot = {
    header,
    revision: SessionPersistenceRevision('jsonl:archived-gate'),
  }

  const followup = vi.fn(() => Promise.resolve())
  const flush = vi.fn(() => Promise.resolve())
  const unarchive = vi.fn(async () => { archivedIds.splice(0, archivedIds.length) })
  const emitted: { method: string, params: Record<string, unknown> }[] = []
  const archivedIds: string[] = archived ? [String(sessionId)] : []

  const agent: Agent = {
    id: sessionId,
    options: { provider: originalModel.provider, model: originalModel.model },
    session: { id: sessionId, events } as unknown as Agent['session'],
    inbox: { nextTurn: [], nextStep: [] } as unknown as Agent['inbox'],
    status: 'idle',
    ctx: {} as Context,
    cancel: vi.fn(),
    whenIdle: () => Promise.resolve(),
    runMaintenance: operation => operation(new AbortController().signal),
    send: vi.fn(),
    followup,
    steer: vi.fn(),
    inject: vi.fn(),
  }

  const modelItems = [originalModel].map(selection => ({
    selectionId: modelSelectionId(selection),
    provider: selection.provider,
    model: selection.model,
    reasoningEffort: null,
    name: selection.model,
    enabled: true,
  }))
  const catalogSnapshot: CatalogSnapshot = {
    revision: 1,
    models: modelItems,
    permissions: ['workspace-write'].map(preset => ({
      selectionId: permissionSelectionId(preset),
      preset,
      name: preset,
      enabled: true,
    })),
  }
  const catalogs = {
    current: () => Promise.resolve(catalogSnapshot),
    defaultModel: () => Promise.resolve(originalModel),
    resolveModel: (id: string) => Promise.resolve(
      id === modelSelectionId(originalModel) ? originalModel : undefined,
    ),
    resolvePermission: (id: string) => Promise.resolve(
      id === permissionSelectionId('workspace-write') ? 'workspace-write' : undefined,
    ),
    permissionFor: () => permissionSelectionId('workspace-write'),
  } as unknown as CatalogManager

  const agents = {
    get: () => agent,
    resume: vi.fn(async (options: { setup?(ctx: Context): void }) => {
      const scopedContext = {
        on: () => () => undefined,
      } as unknown as Context
      options.setup?.(scopedContext)
      return { agent, dispose: () => Promise.resolve() } satisfies AgentHandle
    }),
  }

  const ctx = {
    agents,
    sessions: { flush },
    sessionPersistence: {
      listSnapshots: () => Promise.resolve([snapshot]),
      inspect: () => Promise.resolve({ meta: header, events }),
    },
    permissionPresets: {
      current: () => 'workspace-write',
      set: vi.fn(),
    },
    llm: { listProviders: () => [{ id: 'test' }] },
    workspaceRegistry: {
      get archivedSessionIds() { return [...archivedIds] },
      ...(options.unarchiveSupported ? { unarchiveSession: unarchive } : {}),
    },
  } as unknown as Context

  const root = await mkdtemp(join(tmpdir(), 'aa-bridge-archived-'))
  roots.push(root)
  const metadata = new MetadataStore(root)
  await metadata.initialize()
  const manager = new SessionManager(
    ctx,
    metadata,
    catalogs,
    10,
    10,
    (method, params) => { emitted.push({ method, params: params as Record<string, unknown> }) },
    () => undefined,
    () => undefined,
  )

  return {
    manager,
    platformSessionId,
    sessionId,
    unarchive,
    followup,
    emitted,
  }
}
