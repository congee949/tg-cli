"""Incremental, local-only archive of photos and documents in one chat."""

import asyncio
import json
import os
import re
import sqlite3
import time
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import click
from telethon import utils
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

from ..client import connect
from ._chat import _parse_chat

MAX_BYTES = 64 * 1024 * 1024
MAX_ATTEMPTS = 3
APPLICATION_ID = 0x54474D53  # TGMS
SCHEMA_VERSION = 1
SCHEMA = (
    """CREATE TABLE messages (
        chat_id INTEGER, msg_id INTEGER, date TEXT NOT NULL, text TEXT, media_type TEXT,
        media_path TEXT, media_id INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(chat_id, msg_id)
    )""",
    """CREATE INDEX media_pending ON messages(chat_id, msg_id)
        WHERE media_path IS NULL""",
    """CREATE TABLE cursors (
        chat_id INTEGER, since TEXT, last_id INTEGER NOT NULL, PRIMARY KEY(chat_id, since)
    )""",
    "CREATE TABLE cooldown (singleton INTEGER PRIMARY KEY, until REAL NOT NULL)",
)


def open_archive(db):
    # Serialize first open. Ownership, version and schema must agree before writing.
    with db:
        db.execute("BEGIN IMMEDIATE")
        app_id, = db.execute("PRAGMA application_id").fetchone()
        version, = db.execute("PRAGMA user_version").fetchone()
        objects = {sql for (sql,) in db.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"
        )}
        if not objects and app_id == version == 0:
            for statement in SCHEMA:
                db.execute(statement)
            db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        elif (app_id, version, objects) != (APPLICATION_ID, SCHEMA_VERSION, set(SCHEMA)):
            raise click.ClickException("Unrecognized media archive identity, version or schema.")


def timestamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def restriction(msg):
    if msg is None:
        return "unavailable"
    if getattr(msg, "noforwards", False):
        return "protected"
    if getattr(getattr(msg, "media", None), "ttl_seconds", None) is not None:
        return "ephemeral"
    ttl = getattr(msg, "ttl_period", None)
    if ttl and time.time() >= msg.date.timestamp() + ttl:
        return "expired"
    return None


def attachment(msg):
    media = getattr(msg, "media", None)
    if isinstance(media, MessageMediaPhoto) and msg.photo:
        return msg.photo
    if isinstance(media, MessageMediaDocument) and msg.document:
        return msg.document
    return None


class TooLarge(Exception):
    pass


def check_size(received, total):
    if max(received, total or 0) > MAX_BYTES:
        raise TooLarge


async def stage_download(client, msg, folder, cleanup):
    size = msg.file.size
    check_size(0, size)
    ext = msg.file.ext or ""
    suffix = ext.lower() if re.fullmatch(r"\.[a-zA-Z0-9]{1,16}", ext) else ".bin"
    folder.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=folder, prefix=f".{msg.id}-", suffix=".part",
                            delete=False) as stream:
        part = Path(stream.name)
        cleanup.callback(part.unlink, missing_ok=True)
        await client.download_media(msg, file=stream, progress_callback=check_size)
        actual = stream.tell()
        check_size(actual, size)
        if not actual or (msg.document and actual != size):
            raise ValueError("incomplete media")
    return part, folder / f"{msg.id}{suffix}"


async def transfer(db, client, peer, folder, key, counts, msg=None):
    pending = ("SELECT media_id, media_type, attempts FROM messages "
               "WHERE chat_id=? AND msg_id=? AND media_path IS NULL")
    row = db.execute(pending, key).fetchone()
    if row is None:
        return
    chat_id, msg_id = key
    media_id, media_type, attempts = row
    staged = error = reason = None
    with ExitStack() as cleanup:
        try:
            if msg is None:
                msg = await client.get_messages(peer, ids=msg_id)
            reason = restriction(msg)
            if not reason:
                media = attachment(msg)
                if (media is None or media.id != media_id or type(msg.media).__name__ != media_type
                        or msg.chat_id != chat_id or msg.id != msg_id):
                    reason = "unavailable"
                else:
                    staged = await stage_download(client, msg, folder, cleanup)
        except FloodWaitError:
            raise
        except TooLarge:
            reason = "too_large"
        except (RPCError, OSError, ValueError) as exc:
            error = type(exc).__name__

        # Another caller may have finished while we were awaiting Telegram.
        with db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(pending, key).fetchone()
            if current is None or current[:2] != row[:2]:
                return
            if staged:
                reason = restriction(msg)
                if not reason:
                    part, final = staged
                    try:
                        part.replace(final)
                    except OSError as exc:
                        error = type(exc).__name__
            if error:
                if attempts != current[2]:
                    return  # Another caller already charged this retry.
                attempts += 1
                path = f"error:{error}" if attempts >= MAX_ATTEMPTS else None
                outcome = "failed" if path else "retry_pending"
            elif reason:
                path, outcome = f"skipped:{reason}", "skipped"
            else:
                path, outcome = str(final), "downloaded"
            db.execute(
                "UPDATE messages SET media_path=?, attempts=? WHERE chat_id=? AND msg_id=?",
                (path, max(attempts, current[2]), *key),
            )
    counts[outcome] += 1


async def sync(db, client, chat, since, output, limit, counts):
    peer = await client.get_entity(_parse_chat(chat))
    if getattr(peer, "noforwards", False):
        raise click.ClickException("This chat restricts saving content.")
    chat_id = utils.get_peer_id(peer)
    folder = output / "media" / str(chat_id)
    scope = chat_id, timestamp(since)
    row = db.execute("SELECT last_id FROM cursors WHERE chat_id=? AND since=?", scope).fetchone()
    last_id = row[0] if row else 0
    counts.update(chat_id=chat_id, last_id=last_id)
    pending = db.execute(
        "SELECT msg_id FROM messages WHERE chat_id=? AND date>=? AND msg_id<=? "
        "AND media_path IS NULL ORDER BY msg_id LIMIT ?", (*scope, last_id, limit)
    ).fetchall()
    for (msg_id,) in pending:
        await transfer(db, client, peer, folder, (chat_id, msg_id), counts)

    if not row:
        # Anchor before the inclusive boundary, including ties at the same second.
        async for prior in client.iter_messages(peer, offset_date=since, limit=1):
            if prior.date < since:
                last_id = prior.id
    async for msg in client.iter_messages(peer, reverse=True, min_id=last_id, limit=limit):
        if msg.chat_id != chat_id:
            raise click.ClickException("History returned a message outside the selected chat.")
        media = attachment(msg) if msg.date >= since else None
        if media is not None and restriction(msg):
            counts["skipped"] += 1
            media = None
        with db:
            if media is not None:
                db.execute(
                    "INSERT INTO messages(chat_id,msg_id,date,text,media_type,media_id) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(chat_id,msg_id) DO NOTHING",
                    (chat_id, msg.id, timestamp(msg.date), msg.message,
                     type(msg.media).__name__, media.id),
                )
            db.execute(
                "INSERT INTO cursors VALUES(?,?,?) ON CONFLICT(chat_id,since) DO UPDATE "
                "SET last_id=max(last_id,excluded.last_id)", (*scope, msg.id),
            )
        counts.update(scanned=counts["scanned"] + 1, last_id=msg.id)
        if media is not None:
            await transfer(db, client, peer, folder, (chat_id, msg.id), counts, msg)


async def run(chat, since, output, limit, counts):
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = output / "messages.db"
    try:
        os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    except FileExistsError:
        pass
    with closing(sqlite3.connect(path)) as db:
        open_archive(db)
        row = db.execute("SELECT until FROM cooldown WHERE singleton=1").fetchone()
        if row and row[0] > time.time():
            raise click.ClickException(f"Flood wait until {row[0]:.0f} (Unix time).")
        try:
            async with connect() as client:
                await sync(db, client, chat, since, output, limit, counts)
        except FloodWaitError as exc:
            with db:
                db.execute(
                    "INSERT INTO cooldown VALUES(1,?) ON CONFLICT(singleton) DO UPDATE "
                    "SET until=max(until,excluded.until)", (time.time() + exc.seconds,)
                )
            raise click.ClickException(f"Flood wait: retry after {exc.seconds}s.") from exc


@click.command("sync-media")
@click.option("--chat", required=True, help="One Telegram username or marked chat ID.")
@click.option("--since", required=True, help="Inclusive ISO 8601 timestamp with UTC offset.")
@click.option("--output", required=True, type=click.Path(file_okay=False, path_type=Path))
@click.option("--limit", required=True, type=click.IntRange(min=1),
              help="History messages per call, plus at most this many queued retries.")
def tg_sync_media(chat, since, output, limit):
    """Sync readable photos and documents to a dedicated local archive."""
    if not chat.strip():
        raise click.BadParameter("Specify one chat.", param_hint="--chat")
    try:
        iso = since[:-1] + "+00:00" if since.endswith("Z") else since
        instant = datetime.fromisoformat(iso)
        if instant.utcoffset() is None:
            raise ValueError
    except ValueError as exc:
        raise click.BadParameter("Expected ISO 8601 with timezone.", param_hint="--since") from exc
    counts = dict(scanned=0, downloaded=0, skipped=0, retry_pending=0, failed=0)
    try:
        asyncio.run(run(chat.strip(), instant, output.resolve(), limit, counts))
    except (OSError, sqlite3.Error, ValueError, RPCError) as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        click.echo(json.dumps(counts))
