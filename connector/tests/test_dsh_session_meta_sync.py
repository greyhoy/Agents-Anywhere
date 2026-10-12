from __future__ import annotations

from connector.runtimes.dsh.bridge.models import session_meta


def test_session_meta_folds_top_level_timeline_sync_facts_into_metadata_sync() -> None:
    meta = session_meta({
        "sessionId": "sess_dsh_1",
        "externalSessionId": "session-abc",
        "runtime": "dsh",
        "title": "Old DSH session",
        "cwd": "/repo",
        "orderingTime": "2026-10-12T10:00:00Z",
        "revision": "jsonl:42",
        "requiresTimelineSync": True,
        "metadata": {"hidden": False, "localArchived": False},
    })
    # The scanner contract (shared with Claude/Codex readers) reads
    # metadata.sync; the bridge reports these facts on the meta top level.
    assert meta.metadata["sync"] == {
        "requires_timeline_sync": True,
        "marker": "jsonl:42",
    }
    # Existing visibility metadata is preserved untouched.
    assert meta.metadata["hidden"] is False
    assert meta.metadata["localArchived"] is False
    assert meta.session_id == "sess_dsh_1"
    assert meta.external_session_id == "session-abc"


def test_session_meta_merges_into_existing_sync_block_without_clobbering() -> None:
    meta = session_meta({
        "sessionId": "sess_dsh_2",
        "externalSessionId": "session-def",
        "runtime": "dsh",
        "requiresTimelineSync": False,
        "metadata": {
            "localArchived": True,
            "sync": {"key": "dsh/sync/session-def", "changed": False},
        },
    })
    assert meta.metadata["sync"] == {
        "key": "dsh/sync/session-def",
        "changed": False,
        "requires_timeline_sync": False,
    }


def test_session_meta_without_timeline_facts_keeps_metadata_untouched() -> None:
    meta = session_meta({
        "sessionId": "sess_dsh_3",
        "externalSessionId": "session-ghi",
        "runtime": "dsh",
        "metadata": {"hidden": True},
    })
    assert "sync" not in meta.metadata
    assert meta.metadata == {"hidden": True}


def test_session_meta_ignores_non_bool_non_string_timeline_facts() -> None:
    meta = session_meta({
        "sessionId": "sess_dsh_4",
        "externalSessionId": "session-jkl",
        "runtime": "dsh",
        "requiresTimelineSync": "yes",
        "revision": 42,
        "metadata": {},
    })
    assert "sync" not in meta.metadata
