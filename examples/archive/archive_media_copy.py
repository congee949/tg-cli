#!/usr/bin/env python3
"""Clone local media into a scoped APFS backup and maintain a retryable manifest."""

import ctypes
import errno
import json
import os
import re
import sqlite3
import stat
import sys
import time
from collections import Counter
from contextlib import closing, contextmanager
from os import scandir
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

APP_ID = 0x54474D32
NUMBERED = re.compile(r"([1-9][0-9]{0,18})(?:\.[a-zA-Z0-9]{1,16})?\Z")
RETRY = 300
DAY = 86400
PAGE = 256

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS numbered(msg_id INTEGER PRIMARY KEY,path TEXT)",
    "CREATE TABLE IF NOT EXISTS pending(msg_id INTEGER PRIMARY KEY,due INTEGER NOT NULL)",
    "CREATE INDEX IF NOT EXISTS due_order ON pending(due,msg_id)",
    """CREATE TABLE IF NOT EXISTS copies(msg_id INTEGER,source TEXT,original_path TEXT,
        backup_name TEXT UNIQUE,bytes INTEGER,inode INTEGER,PRIMARY KEY(msg_id,source))""",
)


def setting(db, key, default):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def save(db, key, value):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value)))


def queue(db, mid, origin, now):
    db.execute(
        """INSERT OR IGNORE INTO pending SELECT ?,? WHERE NOT EXISTS
        (SELECT 1 FROM copies WHERE msg_id=? AND (source='db' OR source=?))""",
        (mid, now, mid, origin),
    )


def dir_stamp(roots):
    parts = []
    for root in roots:
        try:
            st = root.lstat()
        except FileNotFoundError:
            parts.append("-")
            continue
        if not stat.S_ISDIR(st.st_mode):
            raise ValueError("invalid media directory")
        parts.append(f"{st.st_dev}-{st.st_ino}-{st.st_mtime_ns}")
    return ":".join(parts)


def directories(db, src, roots, home, scope, init, now, counts):
    previous = setting(db, "directory", "")
    if not init and now < int(setting(db, "rescan", 0)):
        return previous
    stamp = dir_stamp(roots)
    names = {}
    generic = []
    for root in roots:
        if root.exists():
            with scandir(root) as entries:
                for entry in entries:
                    counts["names_scanned"] += 1
                    match = NUMBERED.fullmatch(entry.name)
                    path = Path(entry.path)
                    if match:
                        mid = int(match[1])
                        if scope.first_msg_id <= mid <= MAX_ID:
                            # Even an invalid second entry makes orphan attribution ambiguous.
                            names[mid] = None if mid in names else str(path)
                    elif entry.is_file(follow_symlinks=False) and entry.stat().st_size:
                        generic.append(str(path))
                        if path.is_relative_to(home):
                            generic.append(str(path.relative_to(home)))
    save(db, "rescan", now + DAY)
    if dir_stamp(roots) != stamp:
        if init:
            raise ValueError("directory changed during initialization")
        counts["unstable_census"] += 1
        return previous
    db.execute("DELETE FROM numbered")
    for mid, raw in names.items():
        if raw is None:
            counts["ambiguous_names"] += 1
            if db_path(src, mid, home, roots, scope)[1]:
                queue(db, mid, "db", now)
        elif verified(raw, mid, home, roots):
            db.execute("INSERT INTO numbered VALUES (?,?)", (mid, raw))
            # A DB path may have appeared behind the forward cursor.
            source = "db" if db_path(src, mid, home, roots, scope)[1] else "orphan"
            queue(db, mid, source, now)
    if generic:
        # One bound JSON array avoids both TEMP writes and the SQL parameter limit.
        rows = src.execute(
            """SELECT msg_id FROM messages WHERE chat_id IN (?,?) AND msg_id>=?
               AND media_path IN (SELECT value FROM json_each(?))""",
            (*scope.peers, scope.first_msg_id, json.dumps(generic)),
        )
        for (mid,) in rows:
            queue(db, mid, "db", now)
    save(db, "directory", stamp)
    return stamp


def scan_db(db, src, home, roots, scope, init, now, counts):
    for peer in scope.peers:
        key = f"cursor{peer}"
        if init:
            # The census already joined existing files to the same DB snapshot.
            # Start polling at the current head without probing historical missing paths.
            (high,) = src.execute(
                "SELECT max(msg_id) FROM messages WHERE chat_id=? AND msg_id>=?",
                (peer, scope.first_msg_id),
            ).fetchone()
            save(db, key, high or scope.first_msg_id - 1)
            continue
        high = int(setting(db, key, scope.first_msg_id - 1))
        rows = src.execute(
            """SELECT msg_id,media_path FROM messages
            WHERE chat_id=? AND msg_id>? ORDER BY msg_id LIMIT ?""",
            (peer, high, PAGE),
        ).fetchall()
        for mid, raw in rows:
            if verified(raw, mid, home, roots):
                queue(db, mid, "db", now)
        counts["db_rows_scanned"] += len(rows)
        if rows:
            save(db, key, rows[-1][0])


def verified(raw, mid, home, roots):
    if not raw:
        return None
    path = home / raw
    if path.parent not in roots or path.parent.is_symlink():
        return None
    match = NUMBERED.fullmatch(path.name)
    if match and int(match[1]) != mid:
        return None
    try:
        info = path.lstat()
    except (FileNotFoundError, NotADirectoryError):
        return None
    return path if stat.S_ISREG(info.st_mode) and info.st_size > 0 else None


def db_path(src, mid, home, roots, scope):
    matches = {
        path
        for (raw,) in src.execute(
            """SELECT media_path FROM messages
               WHERE chat_id IN (?,?) AND msg_id=?""",
            (*scope.peers, mid),
        )
        if (path := verified(raw, mid, home, roots))
    }
    if len(matches) == 1:
        return "db", matches.pop()
    return ("ambiguous" if matches else "absent"), None


def choose(db, src, mid, home, roots, scope, stamp):
    source, path = db_path(src, mid, home, roots, scope)
    if source != "absent":
        return source, path
    if dir_stamp(roots) != stamp:
        return "rescan", None
    row = db.execute("SELECT path FROM numbered WHERE msg_id=?", (mid,)).fetchone()
    return "orphan", verified(row[0], mid, home, roots) if row else None


def clone_fd(fd, directory, name):
    call = ctypes.CDLL(None, use_errno=True).fclonefileat
    call.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
    call.restype = ctypes.c_int
    if call(fd, directory, os.fsencode(name), 0) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def same_bytes(left, right):
    os.lseek(left, 0, os.SEEK_SET)
    while True:
        block = os.read(left, 1024 * 1024)
        if block != os.read(right, 1024 * 1024):
            return False
        if not block:
            return True


def copy_one(path, target, name):
    # Pin the regular source inode. The kernel creates the final name atomically
    # and exclusively. A crash before the SQL commit is reconciled on retry.
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with closing_fd(path, flags) as src, closing_fd(target, os.O_RDONLY | os.O_DIRECTORY) as dest:
        before = os.fstat(src)
        if not stat.S_ISREG(before.st_mode) or not before.st_size:
            raise ValueError("invalid media")
        created = True
        try:
            clone_fd(src, dest, name)
        except FileExistsError:
            created = False
        with closing_fd(name, flags, dir_fd=dest) as result:
            info = os.fstat(result)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_dev != before.st_dev
                or info.st_nlink != 1
                or info.st_ino == before.st_ino
                or info.st_size != before.st_size
                or not created
                and not same_bytes(src, result)
            ):
                raise ValueError("destination conflict")
            return info, created


@contextmanager
def closing_fd(path, flags, **kwargs):
    fd = os.open(path, flags, **kwargs)
    try:
        yield fd
    finally:
        os.close(fd)


def run(home, anti_db, media_dir, backup_dir, scope, limit=32, init=False):
    if sys.platform != "darwin" or limit <= 0:
        raise ValueError("requires macOS and a positive limit")
    home = Path(home).resolve(strict=True)
    anti_db = anti_db.resolve(strict=True)
    roots = tuple(media_dir / str(peer) for peer in scope.peers)
    target = backup_dir.absolute() / scope.subdir
    index = target / "manifest.sqlite3"
    protect_output(index, [anti_db], media_dir, APP_ID)
    identity = source_key(anti_db)
    binding = json.dumps([scope.chat_id, scope.first_msg_id, identity, str(home), str(media_dir)])
    counts = Counter(
        cloned=0, verified_existing=0, failed=0, unavailable=0, db_rows_scanned=0, names_scanned=0
    )
    now = int(time.time())
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    with closing(sqlite3.connect(index, timeout=5)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        (app_id,) = db.execute("PRAGMA application_id").fetchone()
        if app_id != APP_ID:
            if app_id or db.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone():
                raise ValueError("unrecognized manifest")
            for statement in SCHEMA:
                db.execute(statement)
            save(db, "scope", binding)
            db.execute(f"PRAGMA application_id={APP_ID}")
        elif setting(db, "scope", None) != binding:
            raise ValueError("manifest scope or inputs differ")
        with read_transaction(anti_db, identity) as src:
            stamp = directories(db, src, roots, home, scope, init, now, counts)
            scan_db(db, src, home, roots, scope, init, now, counts)
            jobs = db.execute(
                "SELECT msg_id FROM pending WHERE due<=? ORDER BY due,msg_id LIMIT ?", (now, limit)
            ).fetchall()
            for (mid,) in jobs:
                try:
                    source, path = choose(db, src, mid, home, roots, scope, stamp)
                    if path is None:
                        counts["unavailable"] += 1
                        db.execute("DELETE FROM pending WHERE msg_id=?", (mid,))
                        continue
                    saved = db.execute(
                        "SELECT 1 FROM copies WHERE msg_id=? AND source=?", (mid, source)
                    ).fetchone()
                    if not saved:
                        suffix = (
                            path.suffix if re.fullmatch(r"\.[A-Za-z0-9]{1,16}", path.suffix) else ""
                        )
                        name = f"{mid}.{source}{suffix}"
                        info, created = copy_one(path, target, name)
                        db.execute(
                            "INSERT INTO copies VALUES (?,?,?,?,?,?)",
                            (mid, source, str(path), name, info.st_size, info.st_ino),
                        )
                        counts["cloned" if created else "verified_existing"] += 1
                    db.execute("DELETE FROM pending WHERE msg_id=?", (mid,))
                except (OSError, ValueError) as exc:
                    counts["failed"] += 1
                    code = (
                        errno.errorcode.get(exc.errno, "IO")
                        if isinstance(exc, OSError)
                        else "CONFLICT"
                    )
                    counts[f"failure_{code}"] += 1
                    db.execute("UPDATE pending SET due=? WHERE msg_id=?", (now + RETRY, mid))
    return dict(counts)


def main():
    cli = parser(__doc__)
    cli.add_argument(
        "--backup-dir", type=Path, help="parent of scoped backups (default: TGA_HOME/media-backup)"
    )
    cli.add_argument("--init", action="store_true", help="inventory in-scope existing media")
    cli.add_argument("--limit", type=int, default=32, help="maximum attempts per invocation")
    args = cli.parse_args()
    os.umask(0o077)
    try:
        scope = Scope(args.chat_id, args.first_msg_id)
        home, anti, media = paths(args)
        counts = run(
            home,
            anti,
            media,
            args.backup_dir or home / "media-backup",
            scope,
            args.limit,
            args.init,
        )
    except (OSError, ValueError, sqlite3.Error, AttributeError) as exc:
        print(f"Media archive failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(counts, sort_keys=True))
    return int(bool(counts["failed"]))


if __name__ == "__main__":
    sys.exit(main())
