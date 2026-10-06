"""
Hermes Web UI -- Route handlers for GET and POST endpoints.
Extracted from server.py (Sprint 11) so server.py is a thin shell.
"""

import html as _html
import copy
import hashlib
import inspect
import errno
import io
import gzip
import json
from api.sse_chunked import end_sse_headers
import logging
import mimetypes
import os
import queue
import re
import platform
import shlex
import shutil
import sqlite3
import stat as _stat
import subprocess
import sys
import threading
import time
import uuid
import http.client
import socket as _socket
from collections import defaultdict, deque, OrderedDict
from pathlib import Path
from contextlib import closing
from urllib.parse import parse_qs, quote, unquote, urljoin, urlsplit
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener
from api.agent_runtime import (
    AgentRuntimeChangedError,
    agent_runtime_stale_payload,
    ensure_agent_runtime_current,
    require_ai_agent_class,
)
from api.agent_sessions import (
    MESSAGING_SOURCES,
    _looks_like_default_cli_title,
    is_cli_session_row,
    is_cli_session_row_visible,
    read_session_lineage_report,
)
from api.compression_anchor import visible_messages_for_anchor
from api.compression_recovery import (
    COMPRESSION_RECOVERY_ACTION_START_FOCUSED,
    clear_compression_recovery,
    compression_recovery_payload_for_session,
    is_generic_continuation_intent,
)
from api.session_events import (
    add_session_list_changed_listener,
    publish_session_list_changed,
    subscribe_session_events,
    unsubscribe_session_events,
)
from api.gateway_restart import restart_active_profile_gateway
from api.shares import create_or_refresh_share, load_share, revoke_share

logger = logging.getLogger(__name__)


def _publish_session_list_changed(
    reason: str,
    *,
    profile: str | None = None,
    session_id: str | None = None,
) -> None:
    """Publish scoped session changes while tolerating legacy test doubles."""
    if not profile and not session_id:
        publish_session_list_changed(reason)
        return
    try:
        publish_session_list_changed(reason, profile=profile, session_id=session_id)
    except TypeError:
        # Some focused tests monkeypatch the route-level publisher with the
        # historical one-argument or profile-only shape. Preserve the old signal instead of
        # turning unrelated session mutations into 500s.
        if profile:
            try:
                publish_session_list_changed(reason, profile=profile)
                return
            except TypeError:
                pass
        publish_session_list_changed(reason)


def _sync_session_title_to_insights(session) -> None:
    """Write title-only session metadata updates through to state.db when enabled."""
    try:
        if not load_settings().get("sync_to_insights"):
            return
        from api.state_sync import sync_session_usage

        messages = getattr(session, "messages", None) or []
        sync_session_usage(
            session_id=session.session_id,
            input_tokens=getattr(session, "input_tokens", None) or 0,
            output_tokens=getattr(session, "output_tokens", None) or 0,
            estimated_cost=getattr(session, "estimated_cost", 0.0),
            model=getattr(session, "model", ""),
            title=session.title,
            message_count=len(messages),
            profile=getattr(session, "profile", None),
            cache_read_tokens=getattr(session, "cache_read_tokens", None) or 0,
            cache_write_tokens=getattr(session, "cache_write_tokens", None) or 0,
        )
    except Exception:
        logger.debug("Failed to update session title in state.db", exc_info=True)


def _persist_generated_session_title(
    session,
    next_title: str,
    *,
    event_reason: str,
    require_default_title: bool = False,
) -> str:
    normalized_title = str(next_title or "").strip()[:80] or "Untitled"
    sid = str(getattr(session, "session_id", "") or "")
    original_session = session
    with _get_session_agent_lock(sid):
        with LOCK:
            latest = SESSIONS.get(sid)
            if latest is not None and str(getattr(latest, "session_id", "") or "") != sid:
                SESSIONS.pop(sid, None)
                latest = None
            elif latest is not None:
                SESSIONS.move_to_end(sid)
        if latest is None:
            latest = Session.load(sid)
            if latest is None:
                raise KeyError(sid)
        session = _ensure_full_session_before_mutation(sid, latest)
        if getattr(session, "read_only", False):
            raise PermissionError(f"Session {sid} is read-only")
        if require_default_title:
            latest_meta = {
                "title": getattr(session, "title", None),
                "source_tag": getattr(session, "source_tag", None),
                "raw_source": getattr(session, "raw_source", None),
                "session_source": getattr(session, "session_source", None),
                "source_label": getattr(session, "source_label", None),
            }
            if not _looks_like_default_cli_title(latest_meta):
                return session.title
        session.title = normalized_title
        from api.session_ops import mark_session_title_generated

        # mark_session_title_generated sets s.llm_title_generated = True and clears manual_title.
        mark_session_title_generated(session)
        session.save(touch_updated_at=False)
        with LOCK:
            SESSIONS[sid] = session
            SESSIONS.move_to_end(sid)
            _evict_sessions_over_cap()  # #4765: safe LRU eviction (never active/unsaved)
    _sync_session_title_to_insights(session)
    _publish_session_list_changed(
        event_reason,
        profile=getattr(session, "profile", None),
        session_id=sid,
    )
    if original_session is not session:
        original_session.title = session.title
        original_session.llm_title_generated = session.llm_title_generated
        original_session.manual_title = session.manual_title
    return session.title


def _queue_generated_title_for_imported_session(session, cli_meta: dict | None) -> None:
    try:
        cli_meta = dict(cli_meta or {})
        if not session or cli_meta.get("read_only") or not _looks_like_default_cli_title(cli_meta):
            return
        sid = str(getattr(session, "session_id", "") or "")
        if not sid:
            return

        def _run() -> None:
            try:
                current = Session.load(sid)
                if not current:
                    return
                current = _ensure_full_session_before_mutation(sid, current)
                if getattr(current, "read_only", False):
                    return
                current_meta = {
                    "title": getattr(current, "title", None),
                    "source_tag": getattr(current, "source_tag", None),
                    "raw_source": getattr(current, "raw_source", None),
                    "session_source": getattr(current, "session_source", None),
                    "source_label": getattr(current, "source_label", None),
                }
                if not _looks_like_default_cli_title(current_meta):
                    return
                next_title, _reason, _raw_preview = generate_session_title_for_session(current)
                normalized_current = str(getattr(current, "title", "") or "").strip()
                normalized_next = str(next_title or "").strip()
                if not normalized_next or normalized_next == normalized_current:
                    return
                _persist_generated_session_title(
                    current,
                    normalized_next,
                    event_reason="session_title_regenerate",
                    require_default_title=True,
                )
            except Exception:
                logger.debug("Failed to generate imported session title for %s", sid, exc_info=True)

        threading.Thread(target=_run, daemon=True, name=f"imported-title-{sid}").start()
    except Exception:
        logger.debug(
            "Failed to queue imported session title generation for %s",
            getattr(session, "session_id", None),
            exc_info=True,
        )


def _on_session_list_changed(profile: str | None = None) -> None:
    """Invalidate in-process /api/sessions cache when sidebar state mutates."""
    _clear_session_list_cache(profile)
    # #4842: also drop the inner CLI/cron projection cache. While a turn streams
    # that cache is frozen on a stable streaming marker (so per-token message
    # writes don't bust it), which means it no longer self-invalidates via the
    # state.db content fingerprint mid-stream. In-app structural mutations
    # (session create/rename/archive/delete/branch/pin/move/import, attention)
    # fire this listener — and never fire per streamed token — so clearing here
    # restores prompt freshness for those without reintroducing the per-poll
    # rebuild the freeze removed. Note: externally-driven changes that do NOT go
    # through this listener (a scheduled cron job completing, or an external CLI
    # writing rows directly) are not cleared here mid-stream; for those the 30s
    # streaming TTL is the backstop — they surface within one streaming-TTL
    # window (≤30s) rather than instantly. That bound is the deliberate
    # latency/CPU trade-off of the freeze.
    try:
        from api.models import clear_cli_sessions_cache
        clear_cli_sessions_cache()
    except Exception:
        logger.debug("Failed to clear CLI sessions cache on session list change", exc_info=True)


try:
    add_session_list_changed_listener(_on_session_list_changed)
except Exception:
    logger.debug("Failed to register session list cache invalidation listener", exc_info=True)


# ── Cron run tracking ────────────────────────────────────────────────────────
# Track job IDs currently being executed so the frontend can poll status.
_RUNNING_CRON_JOBS: dict[str, float] = {}  # job_id → start_timestamp
_RUNNING_CRON_LOCK = threading.Lock()
_CRON_CREATE_SNAPSHOT_LOCK = threading.Lock()
_MANUAL_COMPRESSION_JOBS: dict[str, dict] = {}
_MANUAL_COMPRESSION_JOBS_LOCK = threading.Lock()
_MANUAL_COMPRESSION_JOB_TTL_SECONDS = 10 * 60
_CRON_OUTPUT_CONTENT_LIMIT = 8000
_CRON_OUTPUT_HEADER_CONTEXT = 200
_MESSAGING_RAW_SOURCES = {str(s).strip().lower() for s in MESSAGING_SOURCES}
_MESSAGING_SESSION_METADATA_CACHE: dict[str, object] = {
    "path": None,
    "mtime": None,
    "identity": {},
}
_MESSAGING_SESSION_METADATA_LOCK = threading.Lock()
_STALE_MESSAGING_END_REASONS = {"session_reset", "session_switch"}
_CSP_REPORT_LOGGER = logging.getLogger("csp_report")
_CSP_REPORT_RATE_LIMIT: dict[str, list[float]] = {}
_CSP_REPORT_RATE_LIMIT_LOCK = threading.Lock()
_CSP_REPORT_RATE_LIMIT_WINDOW_SECONDS = 60
_CSP_REPORT_RATE_LIMIT_MAX = 100
_CSP_REPORT_MAX_BODY_BYTES = 64 * 1024
_CLIENT_EVENT_LOGGER = logging.getLogger("client_event")
_CLIENT_EVENT_RATE_LIMIT: dict[str, list[float]] = {}
_CLIENT_EVENT_RATE_LIMIT_LOCK = threading.Lock()
_CLIENT_EVENT_RATE_LIMIT_WINDOW_SECONDS = 60
_CLIENT_EVENT_RATE_LIMIT_MAX = 30
_CLIENT_EVENT_MAX_BODY_BYTES = 4 * 1024
_EXTENSION_SIDECAR_PROXY_MAX_RESPONSE_BYTES = 512 * 1024
_CLIENT_EVENT_ALLOWED_FIELDS = {
    "event": 64,
    "source": 80,
    "session_id": 128,
    "stream_id": 128,
    "visibility_state": 32,
    "url_path": 256,
    "reason": 160,
}


def _normalize_cron_job_ids(job_ids) -> list[str]:
    seen = set()
    normalized = []
    for job_id in job_ids or []:
        jid = str(job_id or "").strip()
        if not jid or jid in seen:
            continue
        seen.add(jid)
        normalized.append(jid)
    return normalized


def _latest_cron_session_info_for_jobs(
    job_ids, completed_job_ids=None
) -> dict[str, dict[str, int | str | None]]:
    """Return newest persisted cron session info keyed by completed cron job id."""
    normalized = _normalize_cron_job_ids(job_ids)
    requested = _normalize_cron_job_ids(completed_job_ids if completed_job_ids is not None else job_ids)
    if not requested:
        return {}
    if not normalized:
        return {jid: {"session_id": "", "message_count": None} for jid in requested}
    db_path = _active_state_db_path()
    if not db_path or not Path(db_path).exists():
        return {jid: {"session_id": "", "message_count": None} for jid in requested}
    try:
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(sessions)")
            session_cols = {row[1] for row in cur.fetchall()}
            if "id" not in session_cols or "source" not in session_cols:
                return {jid: {"session_id": "", "message_count": None} for jid in requested}
            select_message_count = (
                "s.message_count AS message_count"
                if "message_count" in session_cols
                else "NULL AS message_count"
            )
            if "started_at" in session_cols:
                query = f"""
                    SELECT s.id,
                           {select_message_count}
                    FROM sessions s
                    WHERE LOWER(COALESCE(s.source, '')) = 'cron'
                    ORDER BY COALESCE(s.started_at, 0) DESC, s.id DESC  -- newest start, not last activity
                """
            else:
                query = f"""
                    SELECT s.id,
                           {select_message_count}
                    FROM sessions s
                    WHERE LOWER(COALESCE(s.source, '')) = 'cron'
                    ORDER BY s.id DESC
                """
            cur.execute(query)
            results = {
                jid: {"session_id": "", "message_count": None} for jid in requested
            }
            requested_ids = set(requested)
            prefixes = {jid: f"cron_{jid}_" for jid in normalized}
            for row in cur.fetchall():
                sid = str(row["id"] or "")
                if not sid:
                    continue
                matches = [
                    jid
                    for jid in normalized
                    if sid.startswith(prefixes[jid])
                ]
                if matches:
                    jid = max(matches, key=len)
                    if jid not in requested_ids or results[jid]["session_id"]:
                        continue
                    results[jid] = {
                        "session_id": sid,
                        "message_count": (
                            int(row["message_count"])
                            if row["message_count"] is not None
                            else None
                        ),
                    }
                if all(info["session_id"] for info in results.values()):
                    break
            return results
    except sqlite3.Error:
        return {jid: {"session_id": "", "message_count": None} for jid in requested}



def _session_field(session, field, default=None):
    if isinstance(session, dict):
        return session.get(field, default)
    return getattr(session, field, default)


def _session_counts_toward_pin_quota(session) -> bool:
    """Return True when a pinned session should consume visible pin quota."""
    if not _session_field(session, "pinned", False):
        return False
    if _session_field(session, "archived", False):
        return False
    if isinstance(session, dict):
        row = session
    elif hasattr(session, "compact"):
        row = session.compact()
    else:
        row = {
            "pre_compression_snapshot": _session_field(session, "pre_compression_snapshot", False),
            "source_tag": _session_field(session, "source_tag", None),
            "default_hidden": _session_field(session, "default_hidden", False),
        }
    return not _hide_from_default_sidebar(row)


def _session_row_lineage_root_id(session, sessions_by_id) -> str:
    sid = str(_session_field(session, "session_id", "") or "")
    explicit = _session_field(session, "_lineage_root_id", None)
    if explicit:
        return str(explicit)
    # A branch/fork is an independent, separately-visible session (it carries a
    # parent_session_id purely for provenance), so it must count as its OWN pin
    # lineage — only compression/continuation rows should collapse to a shared
    # root. Without this, two pinned forks of the same parent would collapse to a
    # single quota lineage and let the user exceed pinned_sessions_limit (#3288).
    if _session_field(session, "session_source", None) == "fork":
        return sid
    current = sid
    seen = {sid} if sid else set()
    parent = _session_field(session, "parent_session_id", None)
    while parent:
        parent = str(parent)
        if parent in seen:
            break
        current = parent
        seen.add(parent)
        parent_row = sessions_by_id.get(parent)
        if not parent_row:
            break
        parent = _session_field(parent_row, "parent_session_id", None)
    return current or sid


def _visible_pinned_lineage_ids(session_rows) -> set[str]:
    sessions_by_id = {}
    for row in session_rows:
        sid = str(_session_field(row, "session_id", "") or "")
        if sid:
            sessions_by_id[sid] = row
    roots: set[str] = set()
    for row in session_rows:
        if not _session_counts_toward_pin_quota(row):
            continue
        root = _session_row_lineage_root_id(row, sessions_by_id)
        if root:
            roots.add(root)
    return roots


# ── Profile-scoped session/project filtering (#1611, #1614) ────────────────
#
# Sessions and projects are stored in the WebUI sidecar without per-row
# isolation by default — they're tagged with a `profile` field but every
# query saw all rows. The fix scopes both endpoints to the active profile
# by default, with `?all_profiles=1` opting into aggregate mode.
#
# Renamed-root profile handling (#1612): a row tagged `profile='default'`
# matches the active root regardless of the root's display name, and a row
# tagged with the renamed-root display name (e.g. 'kinni') likewise matches
# when the active profile is `'default'`. _is_root_profile() is the
# canonical check.

# Canonical helper now lives in api.profiles so out-of-process consumers
# (mcp_server.py) can import it without duplicating the visibility model.
# Re-exported here so existing `_profiles_match(...)` call sites in this
# module keep resolving without per-call-site refactors.
from api.profiles import (  # noqa: F401, E402  (re-export)
    _profiles_match,
    _is_isolated_profile_mode,
    _is_root_profile,
    _SKILLS_STATS_CACHE,
    get_active_profile_name,
    get_active_profile_name as _get_active_profile_name,
    get_active_hermes_home,
    list_profiles_api,
    profile_scope_for_detached_worker,
)


def _all_profiles_query_flag(parsed_url) -> bool:
    """Return True if the request URL has `?all_profiles=1` (or true/yes).

    Centralizes the opt-in parsing so /api/sessions and /api/projects use
    the same shape. Accepts 1/true/yes (case-insensitive) for ergonomics.
    """
    qs = parse_qs(parsed_url.query)
    raw = qs.get('all_profiles', [''])[0].strip().lower()
    return raw in ('1', 'true', 'yes', 'on')


def _all_profiles_enabled(parsed_url) -> bool:
    """Enable aggregate profile reads only when the request asks and mode allows it."""
    return _all_profiles_query_flag(parsed_url) and not _is_isolated_profile_mode()


def _query_flag(parsed_url, name: str) -> bool:
    """Return True for a truthy query flag value."""
    qs = parse_qs(parsed_url.query)
    raw = qs.get(name, [''])[0].strip().lower()
    return raw in ('1', 'true', 'yes', 'on')


def _query_positive_int(parsed_url, name: str, *, default=None, maximum: int | None = None):
    """Return a non-negative integer query parameter, or default when absent/invalid."""
    qs = parse_qs(parsed_url.query)
    raw = qs.get(name, [''])[0]
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if value < 0:
        return default
    if maximum is not None:
        value = min(value, int(maximum))
    return value


def _session_visible_to_active_profile(session_profile, handler=None) -> bool:
    """Return whether a detail-load session belongs to the active profile.

    Real request handlers must enforce the same profile boundary as
    /api/sessions, even when the request has no hermes_profile cookie and the
    process-level active profile is the default/root profile. Direct unit-callers
    without a request handler keep the historical metadata-load behavior.
    """
    if handler is None:
        return True
    active_profile = _get_active_profile_name()
    if not isinstance(session_profile, str):
        session_profile = None
    return _profiles_match(session_profile, active_profile)


def _is_profile_agnostic_foreign_session(cli_meta) -> bool:
    """Return whether a foreign-session row lives outside the Hermes profile tree.

    Claude Code transcripts are scanned straight out of ``~/.claude/projects``
    by ``get_claude_code_sessions()``, which stamps ``profile: None`` on every
    row because the JSONL files belong to no Hermes profile at all. The sidebar
    lists them under whichever profile is active, but ``_profiles_match``
    coerces ``None`` to ``'default'``, so the detail-load profile gate 404s
    every one of them as soon as the active profile is a named (non-root) one —
    the session shows in the list and then renders "Session not available in
    web UI." when clicked.

    Exempt these profile-less external-agent rows from the gate so opening one
    behaves identically on the root profile and on named profiles. Rows that
    DO carry a profile (every state.db-backed CLI/messaging/cron session) stay
    fully scoped.
    """
    if not isinstance(cli_meta, dict):
        return False
    if cli_meta.get("profile"):
        return False
    sources = {
        str(cli_meta.get("source_tag") or "").strip().lower(),
        str(cli_meta.get("raw_source") or "").strip().lower(),
    }
    # Profile-less external-agent rows that live outside the Hermes profile tree.
    # Claude Code: scanned from ~/.claude/projects; Codex: scanned from ~/.codex/
    profile_agnostic_sources = {CLAUDE_CODE_SOURCE}
    try:
        from api.codex_sessions import CODEX_SOURCE
        profile_agnostic_sources.add(CODEX_SOURCE)
    except ImportError:
        pass
    return bool(sources & profile_agnostic_sources)


def _request_session_visibility_exempt(method: str, path: str | None) -> bool:
    if not path:
        return False
    if method == "GET" and path == "/api/session":
        # Detail-load owns profile mismatch handling so the frontend can switch
        # to the session's profile instead of treating a valid cross-profile
        # deep link as a deleted/stale session.
        return True
    if method != "POST":
        return False
    # Import routes create/claim sessions before normal ownership exists, and
    # chat/start has inline placeholder-retag rules that must run before the
    # generic request-session guard.
    return path in {
        "/api/session/import",
        "/api/session/import_cli",
        "/api/chat/start",
    }


def _session_id_visible_to_request_profile(handler, sid, *, emit_error: bool = True) -> bool:
    """Return whether ``sid`` belongs to the active profile."""
    if not isinstance(sid, str) or not sid:
        return True
    if not is_safe_session_id(sid):
        return True
    try:
        session = get_session(sid, metadata_only=True)
    except KeyError:
        return True
    if not _session_visible_to_active_profile(getattr(session, "profile", None), handler):
        if emit_error:
            bad(handler, "Session not found", 404)
        return False
    return True


def _stream_id_owner_session_id(stream_id: str | None) -> str | None:
    """Resolve stream owner session_id via active-run registry first, fallback to journal."""
    stream_id = str(stream_id or "").strip()
    if not stream_id:
        return None
    try:
        with ACTIVE_RUNS_LOCK:
            raw = (ACTIVE_RUNS or {}).get(stream_id)
        if isinstance(raw, dict):
            owner = str(raw.get("session_id") or "").strip()
            if owner:
                return owner
    except Exception:
        logger.debug("Failed reading ACTIVE_RUNS owner for stream %s", stream_id, exc_info=True)
    try:
        owner = stream_owner_session_id(stream_id)
        if owner:
            return owner
    except Exception:
        logger.debug("Failed reading registered owner for stream %s", stream_id, exc_info=True)
    if not is_safe_session_id(stream_id):
        return None
    try:
        summary = find_run_summary(stream_id)
        if isinstance(summary, dict):
            owner = str(summary.get("session_id") or "").strip()
            return owner or None
    except Exception:
        logger.debug("Failed reading run summary for stream %s", stream_id, exc_info=True)
    return None


def _stream_id_visible_to_request_profile(
    handler,
    stream_id: str | None,
    *,
    emit_error: bool = True,
) -> bool:
    """Return whether the stream owner is visible to the request's profile."""
    owner_session_id = _stream_id_owner_session_id(stream_id)
    if not owner_session_id:
        return True
    return _session_id_visible_to_request_profile(handler, owner_session_id, emit_error=emit_error)


def _guard_request_session_visibility(handler, parsed, body=None, method="GET") -> bool:
    """Apply request session-profile visibility check to request-supplied IDs.

    Covers top-level `session_id` in the query/body. Routes that accept session
    IDs under other keys must enforce their own visibility checks.
    """
    method = str(method).upper()
    if _request_session_visibility_exempt(method, getattr(parsed, "path", "")):
        return True
    sid = parse_qs(getattr(parsed, "query", "") or "").get("session_id", [None])[0]
    if not _session_id_visible_to_request_profile(handler, sid):
        return False
    if isinstance(body, dict) and not _session_id_visible_to_request_profile(handler, body.get("session_id")):
        return False
    return True


def _active_skills_dir() -> Path:
    """Return the skills directory for the request's active Hermes profile.

    WebUI profile switches are cookie/thread-local scoped, so the agent
    module-level ``tools.skills_tool.SKILLS_DIR`` can still point at the server
    startup profile. Skills UI endpoints must derive the directory from
    ``get_active_hermes_home()`` for every request instead of reading that
    process-global constant.
    """
    try:
        from api.profiles import get_active_hermes_home

        return Path(get_active_hermes_home()) / "skills"
    except Exception:
        try:
            from tools.skills_tool import SKILLS_DIR

            return Path(SKILLS_DIR)
        except Exception:
            return Path(os.getenv("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser() / "skills"


def _skill_path_within(base_dir: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(base_dir.resolve())
        return True
    except (OSError, ValueError):
        return False


def _skill_category_from_path(
    skill_md: Path,
    skills_dirs: list[Path],
    local_skills_dir: Path | None = None,
) -> str | None:
    """Return the UI category for a discovered skill path.

    Flat skills directly under the active *local* skills root stay uncategorized,
    while flat skills under an *external* root use that root's directory name as
    their category. ``local_skills_dir`` identifies the local root explicitly; if
    omitted it falls back to ``skills_dirs[0]`` for backward compatibility, but
    callers should pass it directly because the local root can be filtered out of
    ``skills_dirs`` (e.g. when it does not exist yet on a host with only external
    skills configured), which would otherwise misclassify the first external root
    as local.
    """
    if local_skills_dir is None:
        local_skills_dir = skills_dirs[0] if skills_dirs else None
    for skills_dir in skills_dirs:
        try:
            rel_path = skill_md.relative_to(skills_dir)
        except ValueError:
            continue
        parts = rel_path.parts
        if len(parts) >= 3:
            return parts[0]
        if len(parts) >= 2 and local_skills_dir is not None and skills_dir != local_skills_dir:
            return skills_dir.name
        return None
    return None


def _active_skill_search_dirs(skills_dir: Path) -> list[Path]:
    dirs = [skills_dir]
    try:
        from agent.skill_utils import get_external_skills_dirs

        dirs.extend(Path(p) for p in get_external_skills_dirs())
    except Exception:
        pass
    return [p for p in dirs if p.exists()]


def _worktree_retained_payload(session) -> dict:
    """Return explicit no-cleanup metadata for worktree-backed session actions."""
    worktree_path = getattr(session, "worktree_path", None) if session else None
    if not worktree_path:
        return {}
    payload = {
        "worktree_retained": True,
        "worktree_path": worktree_path,
    }
    worktree_branch = getattr(session, "worktree_branch", None)
    worktree_repo_root = getattr(session, "worktree_repo_root", None)
    if worktree_branch:
        payload["worktree_branch"] = worktree_branch
    if worktree_repo_root:
        payload["worktree_repo_root"] = worktree_repo_root
    return payload


def _worktree_retained_payload_for_session_id(sid: str) -> dict:
    try:
        return _worktree_retained_payload(get_session(sid, metadata_only=True))
    except KeyError:
        return {}
    except Exception:
        logger.debug("Failed to read worktree metadata for deleted session %s", sid)
        return {}


def _active_profile_config_path() -> Path:
    """Return config.yaml for the request's active WebUI profile.

    Skills endpoints are profile-scoped UI actions: both the visible disabled
    toggle state and toggle writes must follow the cookie/thread-local active
    Hermes home, not process-global HERMES_HOME or HERMES_CONFIG_PATH values
    captured at server startup.
    """
    test_override_module = getattr(_get_config_path, "__module__", "")
    if test_override_module != "api.config":
        return _get_config_path()
    try:
        from api.profiles import get_active_hermes_home

        return Path(get_active_hermes_home()) / "config.yaml"
    except Exception:
        return _get_config_path()


def _get_disabled_skill_names_for_profile() -> set:
    """Read disabled skill names from the active profile's config.yaml.

    Unlike ``tools.skills_tool._get_disabled_skill_names`` which reads from
    the process-global ``HERMES_HOME``, this uses ``_get_config_path()`` which
    resolves against the WebUI's active profile.  Checks
    ``skills.platform_disabled.webui`` first, falling back to
    ``skills.disabled``.
    """
    config_path = _active_profile_config_path()
    if not config_path.exists():
        return set()
    try:
        cfg = _load_yaml_config_file(config_path)
    except Exception:
        return set()
    if not isinstance(cfg, dict):
        return set()
    skills_cfg = cfg.get("skills")
    if not isinstance(skills_cfg, dict):
        return set()
    # Check platform_disabled.webui first (mirrors agent platform resolution)
    platform_disabled = skills_cfg.get("platform_disabled")
    if isinstance(platform_disabled, dict) and "webui" in platform_disabled:
        return _normalize_disabled_set(platform_disabled["webui"])
    return _normalize_disabled_set(skills_cfg.get("disabled"))


def _parse_config_string_list(value) -> list:
    """Decode a config value that may hold a JSON-array string into a list.

    ``hermes config set`` (and JSON-mode editor saves) store lists as quoted
    JSON strings (``'[\"a\",\"b\"]'`` or the Python-literal ``\"['a']\"``), so a
    disabled list read from ``config.yaml`` can arrive as a single string
    instead of a YAML list. Treating it as one literal name makes the Skills
    panel show every skill as enabled and makes the toggle write a destructive
    single-entry list (hermes-webui#7120).

    Reuses ``agent.skill_utils.parse_config_string_list`` (hermes-agent #86661
    fix) when the bundled agent source is importable, and mirrors its logic
    otherwise so the two surfaces cannot drift. A scalar string still means one
    name.
    """
    try:
        from agent.skill_utils import parse_config_string_list

        return parse_config_string_list(value)
    except ImportError:
        pass
    import ast

    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                parsed = ast.literal_eval(stripped)
            except (ValueError, SyntaxError):
                parsed = None
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        return [value]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(item) for item in value]
    return []


def _normalize_disabled_set(values) -> set:
    """Normalize a YAML disabled list into a set of stripped strings."""
    if values is None:
        return set()
    if isinstance(values, str):
        values = _parse_config_string_list(values)
    return {str(v).strip() for v in values if str(v).strip()}


def _skills_list_from_dir(skills_dir: Path, category: str | None = None) -> dict:
    """List skills using an explicit local skills directory.

    This mirrors ``tools.skills_tool.skills_list`` closely, but keeps the local
    scan root explicit so per-client WebUI profile switches do not race on or
    leak through the skills tool's module-global ``SKILLS_DIR``.
    """
    from agent.skill_utils import iter_skill_index_files
    from tools.skills_tool import (
        MAX_DESCRIPTION_LENGTH,
        _EXCLUDED_SKILL_DIRS,
        _parse_frontmatter,
        _sort_skills,
        skill_matches_platform,
    )

    if not skills_dir.exists():
        skills_dir.mkdir(parents=True, exist_ok=True)
        return {
            "success": True,
            "skills": [],
            "categories": [],
            "message": f"No skills found. Skills directory created at {skills_dir}/",
        }

    all_skills = []
    seen_names: set[str] = set()
    disabled = _get_disabled_skill_names_for_profile()
    search_dirs = _active_skill_search_dirs(skills_dir)

    for scan_dir in search_dirs:
        for skill_md in iter_skill_index_files(scan_dir, "SKILL.md"):
            if any(part in _EXCLUDED_SKILL_DIRS for part in skill_md.parts):
                continue
            skill_dir = skill_md.parent
            try:
                content = skill_md.read_text(encoding="utf-8")[:4000]
                frontmatter, body = _parse_frontmatter(content)
                if not skill_matches_platform(frontmatter):
                    continue
                name = frontmatter.get("name", skill_dir.name)[:64]
                if name in seen_names:
                    continue
                description = frontmatter.get("description", "")
                if not description:
                    for line in body.strip().split("\n"):
                        line = line.strip()
                        if line and not line.startswith("#"):
                            description = line
                            break
                if len(description) > MAX_DESCRIPTION_LENGTH:
                    description = description[: MAX_DESCRIPTION_LENGTH - 3] + "..."
                seen_names.add(name)
                all_skills.append(
                    {
                        "name": name,
                        "description": description,
                        "category": _skill_category_from_path(
                            skill_md, search_dirs, local_skills_dir=skills_dir
                        ),
                        "disabled": name in disabled,
                    }
                )
            except (UnicodeDecodeError, PermissionError) as e:
                logger.debug("Failed to read skill file %s: %s", skill_md, e)
            except Exception as e:
                logger.debug(
                    "Skipping skill at %s: failed to parse: %s", skill_md, e, exc_info=True
                )

    if category:
        all_skills = [s for s in all_skills if s.get("category") == category]
    all_skills = _sort_skills(all_skills)
    categories = sorted(set(s.get("category") for s in all_skills if s.get("category")))
    result = {
        "success": True,
        "skills": all_skills,
        "categories": categories,
        "count": len(all_skills),
    }
    if all_skills:
        result["hint"] = "Use skill_view(name) to see full content, tags, and linked files"
    else:
        result["message"] = "No skills found in skills/ directory."
    return result


def _find_skill_in_dirs(name: str, skills_dirs: list[Path]) -> tuple[Path | None, Path | None]:
    """Resolve a WebUI skill name inside explicit skills directories."""
    from agent.skill_utils import iter_skill_index_files
    from tools.skills_tool import _EXCLUDED_SKILL_DIRS, _parse_frontmatter

    raw_name = str(name or "").strip().strip("/")
    if not raw_name:
        return None, None

    candidate_names = [raw_name]
    if ":" in raw_name:
        namespace, bare = raw_name.split(":", 1)
        if namespace and bare:
            candidate_names.append(f"{namespace}/{bare}")

    for skills_dir in skills_dirs:
        if not skills_dir.exists():
            continue
        for candidate_name in candidate_names:
            direct_path = skills_dir / candidate_name
            if not _skill_path_within(skills_dir, direct_path):
                continue
            if direct_path.is_dir() and (direct_path / "SKILL.md").exists():
                return direct_path, direct_path / "SKILL.md"
            legacy_md = direct_path.with_suffix(".md")
            if legacy_md.exists() and _skill_path_within(skills_dir, legacy_md):
                return legacy_md.parent, legacy_md

        for skill_md in iter_skill_index_files(skills_dir, "SKILL.md"):
            if any(part in _EXCLUDED_SKILL_DIRS for part in skill_md.parts):
                continue
            skill_dir = skill_md.parent
            if skill_dir.name == raw_name:
                return skill_dir, skill_md
            try:
                frontmatter, _ = _parse_frontmatter(skill_md.read_text(encoding="utf-8")[:4000])
                if frontmatter.get("name") == raw_name:
                    return skill_dir, skill_md
            except Exception:
                continue

        for legacy_md in skills_dir.rglob("*.md"):
            if legacy_md.name == "SKILL.md":
                continue
            if legacy_md.stem == raw_name and _skill_path_within(skills_dir, legacy_md):
                return legacy_md.parent, legacy_md
    return None, None


def _find_skill_in_dir(name: str, skills_dir: Path) -> tuple[Path | None, Path | None]:
    """Resolve a WebUI skill name inside an explicit skills directory."""
    return _find_skill_in_dirs(name, [skills_dir])


# Cap on the courtesy list of names carried by a skill-not-found reply. The
# bound stays; what it must never do is present a partial list as the whole
# set, because a caller that cannot find its skill in `available_skills` will
# conclude the skill is not installed.
_SKILL_NOT_FOUND_LIST_LIMIT = 20


def _skill_not_found_payload(name: str, skills_dir: Path) -> dict:
    all_names = [s["name"] for s in _skills_list_from_dir(skills_dir).get("skills", [])]
    total = len(all_names)
    available = all_names[:_SKILL_NOT_FOUND_LIST_LIMIT]
    truncated = total > len(available)
    hint = "Use skills_list to see all available skills"
    if truncated:
        hint = f"Showing {len(available)} of {total} skills. {hint}"
    return {
        "success": False,
        "error": f"Skill '{name}' not found.",
        "available_skills": available,
        "available_skills_truncated": truncated,
        "total_skills": total,
        "hint": hint,
    }


def _linked_files_for_skill(skill_dir: Path | None) -> dict:
    if not skill_dir or not (skill_dir / "SKILL.md").exists():
        return {}
    linked_files: dict[str, list[str]] = {}

    references_dir = skill_dir / "references"
    if references_dir.exists():
        refs = [str(f.relative_to(skill_dir)) for f in references_dir.glob("*.md")]
        if refs:
            linked_files["references"] = sorted(refs)

    templates_dir = skill_dir / "templates"
    if templates_dir.exists():
        templates = []
        for ext in ["*.md", "*.py", "*.yaml", "*.yml", "*.json", "*.tex", "*.sh"]:
            templates.extend(str(f.relative_to(skill_dir)) for f in templates_dir.rglob(ext))
        if templates:
            linked_files["templates"] = sorted(set(templates))

    assets_dir = skill_dir / "assets"
    if assets_dir.exists():
        assets = [str(f.relative_to(skill_dir)) for f in assets_dir.rglob("*") if f.is_file()]
        if assets:
            linked_files["assets"] = sorted(assets)

    scripts_dir = skill_dir / "scripts"
    if scripts_dir.exists():
        scripts = []
        for ext in ["*.py", "*.sh", "*.bash", "*.js", "*.ts", "*.rb"]:
            scripts.extend(str(f.relative_to(skill_dir)) for f in scripts_dir.glob(ext))
        if scripts:
            linked_files["scripts"] = sorted(set(scripts))

    return linked_files


def _skill_view_from_file(skill_dir: Path | None, skill_md: Path) -> dict:
    from tools.skills_tool import _parse_frontmatter, _parse_tags, skill_matches_platform

    content = skill_md.read_text(encoding="utf-8")
    frontmatter, _body = _parse_frontmatter(content)
    if not skill_matches_platform(frontmatter):
        return {"success": False, "error": "Skill is not available on this platform."}

    metadata = frontmatter.get("metadata")
    hermes_meta = metadata.get("hermes", {}) if isinstance(metadata, dict) else {}
    tags = _parse_tags(hermes_meta.get("tags") or frontmatter.get("tags", ""))
    related_skills = _parse_tags(
        hermes_meta.get("related_skills") or frontmatter.get("related_skills", "")
    )
    try:
        path = str(skill_md.relative_to((skill_dir or skill_md.parent).parent))
    except ValueError:
        path = str(skill_md)

    return {
        "success": True,
        "name": frontmatter.get("name", skill_md.stem if not skill_dir else skill_dir.name),
        "description": frontmatter.get("description", ""),
        "tags": tags,
        "related_skills": related_skills,
        "content": content,
        "path": path,
        "skill_dir": str(skill_dir) if skill_dir else None,
        "linked_files": _linked_files_for_skill(skill_dir),
    }


def _skill_view_from_active_dir(name: str) -> dict:
    from tools.skills_tool import skill_view as _skill_view

    skills_dir = _active_skills_dir()
    search_dirs = _active_skill_search_dirs(skills_dir)
    skill_dir, skill_md = _find_skill_in_dirs(name, search_dirs)
    if not skill_md:
        # Preserve plugin-qualified skill viewing without falling back to the
        # startup/root profile's local skills tree for ordinary missing skills.
        if ":" in str(name or ""):
            try:
                from agent.skill_utils import is_valid_namespace, parse_qualified_name
                from hermes_cli.plugins import discover_plugins, get_plugin_manager

                namespace, _bare = parse_qualified_name(name)
                if is_valid_namespace(namespace):
                    discover_plugins()
                    pm = get_plugin_manager()
                    if pm.find_plugin_skill(name) is not None or pm.list_plugin_skills(namespace):
                        raw = _skill_view(name)
                        return json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                pass
        return _skill_not_found_payload(name, skills_dir)
    return _skill_view_from_file(skill_dir, skill_md)

# ── SSE app-level heartbeat (#1623) ────────────────────────────────────────
#
# Kernel TCP keepalive (server.py setsockopt block) declares a peer dead at
# KEEPIDLE (10s) + KEEPINTVL (5s) * KEEPCNT (3) = 25s in the worst case. The
# app-level SSE heartbeat must fire well below that window so flaky-network
# probes never get the chance to kill an idle stream during long LLM thinking
# phases. 5s gives the kernel ~5x headroom: probe at 10s, heartbeat byte at
# every 5s of idle keeps the socket warm.
#
# Cost: ~12 bytes per heartbeat * 12 extra heartbeats/min = ~150B/min idle.
# Trivial; many production SSE deployments run 5-15s heartbeats specifically
# to handle proxies and mobile NAT.
_SSE_HEARTBEAT_INTERVAL_SECONDS = 5
_SESSION_SSE_SENT_EVENT_ID_LIMIT = 4096


def _normalize_messaging_source(raw_source) -> str:
    return str(raw_source or "").strip().lower()


def _is_known_messaging_source(raw_source) -> bool:
    return _normalize_messaging_source(raw_source) in _MESSAGING_RAW_SOURCES


def _safe_first(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _gateway_session_metadata_path():
    try:
        from api.profiles import get_active_hermes_home
        hermes_home = Path(get_active_hermes_home()).expanduser().resolve()
    except Exception:
        hermes_home = Path(os.getenv("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser().resolve()
    return hermes_home / "sessions" / "sessions.json"


def _load_gateway_session_identity_map() -> dict[str, dict]:
    path = _gateway_session_metadata_path()
    if not path.exists():
        return {}

    try:
        st = path.stat()
        cache = _MESSAGING_SESSION_METADATA_CACHE
        with _MESSAGING_SESSION_METADATA_LOCK:
            if cache["path"] == str(path) and cache["mtime"] == st.st_mtime:
                return cache["identity"].copy()
    except Exception:
        return {}

    try:
        raw_sessions = json.loads(path.read_text(encoding="utf-8"))
    except Exception as _json_err:
        logger.debug("Failed to parse gateway sessions metadata from %s: %s", path, _json_err)
        return {}

    mapping: dict[str, dict] = {}
    if isinstance(raw_sessions, dict):
        for _entry in raw_sessions.values():
            if not isinstance(_entry, dict):
                continue
            session_id = _safe_first(_entry.get("session_id"))
            if not session_id:
                continue
            origin = _entry.get("origin") if isinstance(_entry.get("origin"), dict) else {}
            platform = _safe_first(origin.get("platform"), _entry.get("platform"))
            mapping[session_id] = {
                "session_key": _safe_first(_entry.get("session_key"), _entry.get("key")),
                "chat_id": _safe_first(origin.get("chat_id"), _entry.get("chat_id")),
                "thread_id": _safe_first(origin.get("thread_id"), _entry.get("thread_id")),
                "chat_type": _safe_first(origin.get("chat_type"), _entry.get("chat_type")),
                "user_id": _safe_first(origin.get("user_id"), _entry.get("user_id")),
                "platform": platform,
                "raw_source": platform,
            }

    with _MESSAGING_SESSION_METADATA_LOCK:
        _MESSAGING_SESSION_METADATA_CACHE["path"] = str(path)
        _MESSAGING_SESSION_METADATA_CACHE["mtime"] = st.st_mtime
        _MESSAGING_SESSION_METADATA_CACHE["identity"] = mapping
    return mapping.copy()


def _gateway_status_payload() -> dict:
    import datetime

    identity_map = _load_gateway_session_identity_map()
    sessions_path = _gateway_session_metadata_path()

    # Detect whether the gateway process is alive, independent of connected
    # messaging platforms. An empty identity_map means zero connected
    # platforms, not necessarily a stopped gateway.
    health = build_agent_health_payload()
    alive = health.get("alive")
    details = health.get("details") if isinstance(health.get("details"), dict) else {}
    health_reason = details.get("reason")
    health_state = details.get("state")
    health_gateway_state = details.get("gateway_state")
    if alive is True:
        running = True
        configured = True
    elif alive is False:
        running = False
        configured = True
    else:
        gateway_running_metadata = (
            health_reason == "gateway_stale_running_state"
            or health_gateway_state == "running"
        )
        configured = True if gateway_running_metadata else bool(identity_map)
        running = bool(identity_map)

    platforms_set: set[str] = set()
    for meta in identity_map.values():
        raw = meta.get("raw_source") or meta.get("platform") or ""
        norm = _normalize_messaging_source(raw)
        if norm:
            platforms_set.add(norm)
    platform_labels = {
        "telegram": "Telegram",
        "discord": "Discord",
        "slack": "Slack",
        "email": "Email",
        "web": "Web",
        "api": "API",
    }
    platforms = sorted(
        [{"name": p, "label": platform_labels.get(p, p.title())} for p in platforms_set],
        key=lambda x: x["label"],
    )
    last_active = ""
    if running and sessions_path.exists():
        try:
            mtime = sessions_path.stat().st_mtime
            last_active = datetime.datetime.fromtimestamp(mtime).isoformat()
        except Exception:
            pass
    return {
        "running": running,
        "configured": configured,
        "platforms": platforms,
        "last_active": last_active,
        "session_count": len(identity_map),
        "health": {
            "state": health_state,
            "reason": health_reason,
            "gateway_state": health_gateway_state,
        },
    }


_GATEWAY_LIFECYCLE_TIMEOUT_SECONDS = 60

# Server-side single-flight guard for gateway lifecycle actions. The client
# disables its button while a request is in flight, but a scripted authed
# client could still fire overlapping start/stop/restart calls, spawning
# concurrent `hermes gateway` subprocesses. Serialize them here (mirrors the
# self-update _apply_lock pattern): a non-blocking acquire returns 409 on
# contention rather than launching a second overlapping subprocess.
_GATEWAY_ACTION_LOCK = threading.Lock()


def _run_gateway_lifecycle_command(action: str) -> subprocess.CompletedProcess:

[Showing lines 1-1300 of 29741 (50.0KB limit). Use offset=1301 to continue.]