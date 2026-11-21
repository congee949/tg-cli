"""Local archive scope, filesystem guards and consistent SQLite input snapshots."""

import argparse
import os
import sqlite3
import unicodedata
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

MAX_ID = 2**63 - 1
CHANNEL_OFFSET = 10**12
DB_SUFFIXES = ("", "-wal", "-shm", "-journal")


@dataclass(frozen=True)
class Scope:
    chat_id: int
    first_msg_id: int

    def __post_init__(self):
        bare = self.chat_id
        if bare < -CHANNEL_OFFSET:
            bare = -bare - CHANNEL_OFFSET
        if not 0 < bare < CHANNEL_OFFSET or not 1 <= self.first_msg_id <= MAX_ID:
            raise ValueError("invalid channel or starting message")
        object.__setattr__(self, "chat_id", bare)

    @property
    def peers(self):
        return self.chat_id, -(CHANNEL_OFFSET + self.chat_id)

    @property
    def subdir(self):
        return Path(str(self.peers[1])) / str(self.first_msg_id)


def parser(description):
    cli = argparse.ArgumentParser(description=description)
    cli.add_argument(
        "--chat-id", type=int, required=True, help="bare or marked channel/supergroup ID"
    )
    cli.add_argument("--first-msg-id", type=int, required=True, help="inclusive lower bound")
    cli.add_argument(
        "--tga-home",
        type=Path,
        default=os.environ.get("TGA_HOME"),
        help="base for relative media paths (default: TGA_HOME)",
    )
    cli.add_argument("--anti-db", type=Path, help="default: TGA_HOME/antidelete.db")
    cli.add_argument(
        "--media-dir", type=Path, help="parent of per-peer directories (default: TGA_HOME/media)"
    )
    return cli


def paths(args):
    if args.tga_home is None:
        raise ValueError("supply --tga-home or set TGA_HOME")
    home = args.tga_home.resolve(strict=True)
    media = args.media_dir or home / "media"
    if media.is_symlink():
        raise ValueError("symlinked media root")
    return home, (args.anti_db or home / "antidelete.db").resolve(strict=True), media.resolve()


def db_paths(path):
    return [Path(str(path) + suffix) for suffix in DB_SUFFIXES]


def folded(path):
    # Also reserve absent sidecar names on case-insensitive filesystems.
    return Path(unicodedata.normalize("NFD", str(path)).casefold())


def protect_output(index, sources, media, app_id):
    if index.resolve() != index or folded(index).is_relative_to(folded(media)):
        raise ValueError("output overlaps media or uses a symlink")
    reserved = {p for src in sources for p in db_paths(src)}
    for p in db_paths(index):
        if p.is_symlink() or any(folded(p).is_relative_to(folded(src)) for src in reserved):
            raise ValueError("output overlaps source or uses a symlink")
        if p.exists() and (
            not p.is_file()
            or p.stat().st_nlink != 1
            or any(src.exists() and p.samefile(src) for src in reserved)
        ):
            raise ValueError("unsafe output file")
    # Opening an unrelated hot SQLite database for writing can recover its
    # journal before schema validation. Check the file before opening SQLite.
    if index.exists() and index.stat().st_size:
        with index.open("rb") as stream:
            stream.seek(68)
            if int.from_bytes(stream.read(4), "big") != app_id:
                raise ValueError("unrecognized output database")


def source_key(path):
    s = path.stat()
    return [str(path), s.st_dev, s.st_ino]


@contextmanager
def read_transaction(path, identity=None):
    uri = path.resolve(strict=True).as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=5)) as db:
        db.execute("PRAGMA trusted_schema=OFF")
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        if identity is not None and source_key(path) != identity:
            raise ValueError("input database was replaced")
        yield db
