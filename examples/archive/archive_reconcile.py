#!/usr/bin/env python3
"""Reconcile two local Telegram message databases into a scoped SQLite archive."""

import json
import os
import sqlite3
import stat
import sys
from contextlib import closing
from pathlib import Path

from archive_common import (
    MAX_ID,
    Scope,
    parser,
    paths,
    protect_output,
    read_transaction,
    source_key,
)

APP_ID = 0x54474132
# Match each source's leading index columns, then seek a bounded msg_id range.
QUERIES = {
    "tg_cli": """SELECT msg_id, timestamp, content,
                       CASE WHEN json_valid(raw_json)
                            THEN json_extract(raw_json, '$.media_type') END, NULL
                  FROM messages WHERE platform='telegram' AND chat_id=?""",
    "antidelete": """SELECT msg_id, date, text, media_type, media_path
                       FROM messages WHERE chat_id=?""",
}
PAGE = " AND msg_id>? AND msg_id<=? ORDER BY msg_id LIMIT ?"

SCHEMA = (
    """CREATE TABLE observations(
        msg_id INTEGER NOT NULL, source TEXT NOT NULL, source_chat_id INTEGER NOT NULL,
        message_date TEXT, text TEXT, media_type TEXT, media_path TEXT, media_state TEXT NOT NULL,
        PRIMARY KEY(msg_id, source, source_chat_id)) WITHOUT ROWID""",
    """CREATE TABLE progress(
        source TEXT NOT NULL, source_chat_id INTEGER NOT NULL,
        high INTEGER NOT NULL, sweep_after INTEGER NOT NULL, sweep_end INTEGER NOT NULL,
        PRIMARY KEY(source, source_chat_id)) WITHOUT ROWID""",
    "CREATE TABLE scope(chat_id INTEGER, first_msg_id INTEGER, inputs TEXT)",
    """CREATE VIEW messages AS SELECT (SELECT chat_id FROM scope) AS chat_id, msg_id,
        max(source='tg_cli') AS has_tg_cli, max(source='antidelete') AS has_antidelete,
        max(CASE WHEN source='tg_cli' THEN text END) AS tg_cli_text,
        max(CASE WHEN source='antidelete' THEN text END) AS antidelete_text,
        (count(DISTINCT text) + (count(text)<count(*)) > 1) AS text_differs,
        max(media_state='present') AS has_local_media
        FROM observations GROUP BY msg_id""",
)

STATUS = """SELECT count(*) AS messages,
    count(*) FILTER (WHERE has_tg_cli AND NOT has_antidelete) AS tg_cli_only,
    count(*) FILTER (WHERE has_antidelete AND NOT has_tg_cli) AS antidelete_only,
    count(*) FILTER (WHERE has_tg_cli AND has_antidelete) AS both_sources,
    count(*) FILTER (WHERE text_differs) AS differing_texts,
    count(*) FILTER (WHERE has_local_media) AS with_local_media FROM messages"""

UPSERT = """INSERT INTO observations VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(msg_id,source,source_chat_id) DO UPDATE SET
        (message_date,text,media_type,media_path,media_state) =
        (excluded.message_date,excluded.text,excluded.media_type,excluded.media_path,excluded.media_state)
    WHERE (message_date,text,media_type,media_path,media_state) IS NOT
        (excluded.message_date,excluded.text,excluded.media_type,excluded.media_path,excluded.media_state)"""


def media_state(kind, raw, base, roots):
    if not raw or raw == "None":
        return "unavailable" if kind or raw else "none_recorded"
    if raw.startswith("error:"):
        return "error"
    if raw.startswith("skipped:"):
        return "skipped"
    try:
        path = base / raw
        if path in roots:
            return "not_file"
        if path.parent not in roots or path.parent.is_symlink() or path.is_symlink():
            return "unverified"
        info = path.lstat()
    except (FileNotFoundError, NotADirectoryError):
        return "missing"
    except (OSError, ValueError):
        return "unverified"
    if not stat.S_ISREG(info.st_mode):
        return "not_file"
    return "present" if info.st_size else "empty_file"


def validate_archive(db, scope, inputs=None):
    (app_id,) = db.execute("PRAGMA application_id").fetchone()
    if app_id == APP_ID:
        row = db.execute("SELECT chat_id,first_msg_id,inputs FROM scope").fetchone()
        if (
            row
            and row[:2] == (scope.chat_id, scope.first_msg_id)
            and (inputs is None or row[2] == inputs)
        ):
            return
        raise ValueError("archive scope or inputs differ")
    if inputs is None or app_id or db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
        raise ValueError("unrecognized archive")
    for statement in SCHEMA:
        db.execute(statement)
    db.execute("INSERT INTO scope VALUES (?,?,?)", (scope.chat_id, scope.first_msg_id, inputs))
    db.execute(f"PRAGMA application_id={APP_ID}")


def sync_stream(out, db, source, base, roots, chat, limit, full, stats, first_id):
    def page(after, end):
        rows = db.execute(QUERIES[source] + PAGE, (chat, after, end, limit)).fetchall()
        before = out.total_changes
        out.executemany(
            UPSERT,
            (
                (mid, source, chat, date, body, kind, path, media_state(kind, path, base, roots))
                for mid, date, body, kind, path in rows
            ),
        )
        stats["source_rows_read"] += len(rows)
        stats["observations_changed"] += out.total_changes - before
        return (rows[-1][0] if rows else after), len(rows)

    prev = out.execute(
        "SELECT high,sweep_after,sweep_end FROM progress WHERE source=? AND source_chat_id=?",
        (source, chat),
    ).fetchone()
    if full or prev is None:
        high = first_id - 1
        while True:
            high, size = page(high, MAX_ID)
            if size < limit:
                break
        after, end = first_id - 1, high
    else:
        high, after, end = prev
        # Freeze the upper bound for an entire sweep so new traffic cannot
        # starve low IDs, even when ingestion exceeds the historical page size.
        high, _ = page(high, MAX_ID)
        after, size = page(after, end)
        if size < limit or after >= end:
            after, end = first_id - 1, high
    if prev != (high, after, end):
        out.execute(
            "INSERT OR REPLACE INTO progress VALUES (?,?,?,?,?)", (source, chat, high, after, end)
        )


def reconcile(tg_db, anti_db, output, scope, home, media, batch_size=512, full=False):
    if batch_size < 1:
        raise ValueError("batch size must be positive")
    inputs = {"tg_cli": tg_db.resolve(strict=True), "antidelete": anti_db.resolve(strict=True)}
    if inputs["tg_cli"].samefile(inputs["antidelete"]):
        raise ValueError("expected two distinct databases")
    output = output.absolute()
    protect_output(output, inputs.values(), media, APP_ID)
    roots = tuple(media / str(peer) for peer in scope.peers)
    identities = {name: source_key(path) for name, path in inputs.items()}
    binding = json.dumps([identities, str(home), str(media)], sort_keys=True)
    stats = {"source_rows_read": 0, "observations_changed": 0}
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with closing(sqlite3.connect(output, timeout=5)) as out, out:
        out.execute("BEGIN IMMEDIATE")
        validate_archive(out, scope, binding)
        for name, path in inputs.items():
            with read_transaction(path, identities[name]) as db:
                for chat in scope.peers:
                    sync_stream(
                        out,
                        db,
                        name,
                        home,
                        roots,
                        chat,
                        batch_size,
                        full,
                        stats,
                        scope.first_msg_id,
                    )
    return stats


def status(path, scope):
    # Reject a misdirected --status before SQLite can open any WAL sidecars.
    with path.open("rb") as stream:
        stream.seek(68)
        if int.from_bytes(stream.read(4), "big") != APP_ID:
            raise ValueError("unrecognized archive")
    with read_transaction(path) as db:
        validate_archive(db, scope)
        db.row_factory = sqlite3.Row
        return dict(db.execute(STATUS).fetchone())


def main():
    cli = parser(__doc__)
    cli.add_argument("--tg-db", type=Path, help="tg-cli message database (required for import)")
    cli.add_argument("--output", type=Path, help="override the scoped archive file")
    cli.add_argument("--batch-size", type=int, default=512)
    mode = cli.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="revisit all source rows now")
    mode.add_argument("--status", action="store_true", help="print only archive coverage counts")
    args = cli.parse_args()
    os.umask(0o077)
    try:
        scope = Scope(args.chat_id, args.first_msg_id)
        if not args.output and not args.tga_home:
            cli.error("supply --output or --tga-home/TGA_HOME")
        output = args.output or args.tga_home.resolve() / "state" / scope.subdir / "archive.sqlite3"
        if args.status:
            counts = status(output, scope)
        else:
            if args.tg_db is None:
                cli.error("supply --tg-db for import")
            home, anti, media = paths(args)
            counts = reconcile(
                args.tg_db, anti, output, scope, home, media, args.batch_size, args.full
            )
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(
            f"Reconciliation failed ({type(exc).__name__}). Check scope, inputs and output.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
