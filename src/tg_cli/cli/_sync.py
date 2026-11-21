"""Shared sync helpers for CLI commands."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from sqlite3 import DatabaseError

from telethon.errors import AuthKeyError, AuthKeyNotFound, FloodWaitError, UnauthorizedError

from ..client import connect, fetch_history, sync_all
from ..db import MessageDB
from ._chat import _parse_chat


async def sync_all_dialogs(
    *,
    limit: int,
    on_chat_done: Callable[[str, int, int], None] | None = None,
    delay: float = 1.0,
    max_chats: int | None = None,
) -> dict[str, int]:
    """Sync all dialogs available to the current Telegram account."""
    with MessageDB() as db:
        async with connect() as client:
            return await sync_all(
                client,
                db,
                limit_per_chat=limit,
                on_chat_done=on_chat_done,
                delay=delay,
                max_chats=max_chats,
            )


async def sync_chat_dialog(
    chat: str,
    *,
    limit: int,
    on_progress: Callable[[int], None] | None = None,
) -> int:
    """Sync a single chat into the local database."""
    with MessageDB() as db:
        async with connect() as client:
            entity = await client.get_entity(_parse_chat(chat))
            return await fetch_history(
                client,
                entity,
                limit=limit,
                db=db,
                on_progress=on_progress,
                min_id=db.get_last_msg_id(entity.id, platform="telegram") or 0,
            )


async def sync_many_dialogs(requests: list[tuple[str, int]]) -> list[dict]:
    """Process requests in order with one connection and a fresh cursor per peer."""
    results = []
    with MessageDB() as db:
        async with connect() as client:
            # Let the poller schedule retries, including short Telegram waits.
            client.flood_sleep_threshold = 0
            for chat, limit in requests:
                results.append(await _sync_request(client, db, chat, limit))
    return results


async def _sync_request(client, db: MessageDB, chat: str, limit: int) -> dict:
    row = {
        "chat": chat,
        "requested_limit": limit,
        "resolved_chat_id": None,
        "last_msg_id": None,
        "synced": 0,
        "status": "failed",
        "retry_after_seconds": None,
        "error": None,
    }
    if len(db.find_chats(chat)) > 1:
        row["error"] = f"Chat '{chat}' matches multiple local chats."
        return row

    def on_progress(count: int):
        row["synced"] = count

    try:
        entity = await client.get_entity(_parse_chat(chat))
        row["resolved_chat_id"] = entity.id
        last_id = db.get_last_msg_id(entity.id, platform="telegram") or 0
        row["last_msg_id"] = last_id
        row["synced"] = await fetch_history(
            client,
            entity,
            limit=limit,
            db=db,
            min_id=last_id,
            on_progress=on_progress,
            propagate_flood_wait=True,
        )
        row["status"] = "ok"
    except FloodWaitError as exc:
        row["status"] = "rate_limited"
        row["retry_after_seconds"] = exc.seconds
        row["error"] = f"Telegram rate limit: retry after {exc.seconds}s"
    except (
        OSError,
        asyncio.TimeoutError,
        AuthKeyError,
        AuthKeyNotFound,
        UnauthorizedError,
        DatabaseError,
    ):
        raise
    except Exception as exc:
        if not client.is_connected():
            raise
        row["error"] = str(exc)
    return row
