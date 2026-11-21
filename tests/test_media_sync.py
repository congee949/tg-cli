"""Offline command-level tests with an in-memory Telegram history."""

import json
import sqlite3
import unittest
from contextlib import asynccontextmanager, closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner
from telethon import utils
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, PeerChannel

from tg_cli.cli import media_sync as sync

START = datetime(2025, 1, 1, tzinfo=timezone.utc)
CHAT = utils.get_peer_id(PeerChannel(123))
OTHER = utils.get_peer_id(PeerChannel(456))


def message(mid, kind="document", *, chat=CHAT, when=START, size=3, ext=".txt"):
    obj = SimpleNamespace(id=100 + mid)
    photo = obj if kind == "photo" else None
    doc = obj if kind == "document" else None
    media = MessageMediaPhoto(photo=photo) if photo else MessageMediaDocument(document=doc)
    return SimpleNamespace(id=mid, chat_id=chat, date=when, media=media if kind else None,
                           message=f"caption {mid}", document=doc, photo=photo,
                           file=SimpleNamespace(ext=ext, size=size))


class Client:
    def __init__(self):
        self.messages = {}
        self.downloads = []
        self.fetches = []
        self.payload = {}
        self.on_download = None
        self.connects = 0

    @asynccontextmanager
    async def connect(self):
        self.connects += 1
        yield self

    async def get_entity(self, chat):
        assert chat in (CHAT, OTHER)
        return PeerChannel(-chat - 10**12)

    async def iter_messages(self, peer, *, limit, offset_date=None, reverse=False, min_id=0):
        rows = [m for m in self.messages[utils.get_peer_id(peer)] if m.id > min_id]
        if offset_date:
            rows = [m for m in rows if m.date < offset_date]
        for msg in sorted(rows, key=lambda m: m.id, reverse=not reverse)[:limit]:
            yield msg

    async def get_messages(self, peer, *, ids):
        chat = utils.get_peer_id(peer)
        self.fetches.append((chat, ids))
        return next((m for m in self.messages[chat] if m.id == ids), None)

    async def download_media(self, msg, file, progress_callback):
        self.downloads.append(msg.id)
        if self.on_download:
            self.on_download(msg)
        payload = self.payload.get(msg.id, b"abc")
        if isinstance(payload, BaseException):
            file.write(b"x")
            raise payload
        file.write(payload)
        progress_callback(file.tell(), msg.file.size)


class MediaSyncTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name).resolve()
        self.client = Client()
        self.client.messages[CHAT] = []
        stub = patch.object(sync, "connect", self.client.connect)
        stub.start()
        self.addCleanup(stub.stop)

    def invoke(self, *, chat=CHAT, since="2025-01-01T00:00:00Z", limit=10, code=0):
        result = CliRunner().invoke(sync.tg_sync_media, [
            "--chat", str(chat), "--since", since, "--output", str(self.output),
            "--limit", str(limit),
        ])
        self.assertEqual(result.exit_code, code, (result.output, repr(result.exception)))
        return result.output

    def query(self, sql):
        with closing(sqlite3.connect(self.output / "messages.db")) as db:
            return db.execute(sql).fetchall()

    def test_pages_scope_and_new_messages(self):
        self.client.messages[CHAT] = [
            message(1, when=START - timedelta(seconds=1)), message(2, None),
            message(3, "photo", ext=".jpg"), message(4, ext="../../escape"),
            message(8, when=START + timedelta(days=1)),
        ]
        self.client.messages[OTHER] = [message(3, chat=OTHER)]
        self.assertEqual(json.loads(self.invoke(limit=2))["last_id"], 3)
        self.invoke(limit=1, since="2025-01-01T08:00:00+08:00")
        self.invoke(since="2025-01-02T00:00:00Z")
        self.invoke(chat=OTHER)
        self.assertEqual(self.query("SELECT count(*) FROM cursors"), [(3,)])
        self.invoke()
        self.assertEqual(json.loads(self.invoke())["scanned"], 0)
        self.client.messages[CHAT].append(message(9, when=START + timedelta(days=2)))
        self.assertEqual(json.loads(self.invoke())["downloaded"], 1)
        self.assertEqual(self.client.downloads, [3, 4, 8, 3, 9])
        rows = self.query("SELECT chat_id,msg_id,date,text,media_type,media_path FROM messages")
        self.assertTrue(all(Path(row[5]).read_bytes() == b"abc" for row in rows))
        row = next(r for r in rows if r[:2] == (CHAT, 4))
        self.assertEqual(row[2:5], (sync.timestamp(START), "caption 4", "MessageMediaDocument"))
        self.assertEqual(Path(row[5]), self.output / "media" / str(CHAT) / "4.bin")

    def test_interruption_after_metadata_commit_then_restart(self):
        self.client.messages[CHAT] = [message(1), message(2)]
        self.client.payload[1] = KeyboardInterrupt()

        def inspect(msg):
            self.assertEqual(self.query("SELECT text,media_path FROM messages"), [
                ("caption 1", None)])
            self.assertEqual(self.query("SELECT last_id FROM cursors"), [(1,)])

        self.client.on_download = inspect
        self.invoke(code=1)
        self.assertEqual(list((self.output / "media" / str(CHAT)).iterdir()), [])
        self.client.on_download = None
        self.client.payload.clear()
        self.assertEqual(json.loads(self.invoke(limit=1))["downloaded"], 2)
        self.assertEqual(self.client.fetches, [(CHAT, 1)])

    def test_size_limits_and_retry_budget(self):
        self.client.messages[CHAT] = [
            message(1), message(2, "photo", size=None), message(3, size=11),
            message(4, "photo", size=None),
        ]
        self.client.payload = {1: b"xx", 2: b"", 4: b"x" * 11}
        with patch.object(sync, "MAX_BYTES", 10):
            first = json.loads(self.invoke())
            self.assertEqual((first["skipped"], first["retry_pending"]), (2, 2))
            self.client.payload[2] = b"ok"
            self.invoke()
            self.assertEqual(json.loads(self.invoke())["failed"], 1)
            self.invoke()
        self.assertEqual(self.client.downloads, [1, 2, 4, 1, 2, 1])
        self.assertEqual(self.query("SELECT attempts,media_path FROM messages WHERE msg_id=1"),
                         [(3, "error:ValueError")])
        folder = self.output / "media" / str(CHAT)
        self.assertEqual([p.read_bytes() for p in folder.iterdir()], [b"ok"])

    def test_retry_checks_current_peer_and_media(self):
        self.client.messages[CHAT] = [message(i) for i in range(1, 4)]
        self.client.payload = dict.fromkeys(range(1, 4), OSError("interrupted"))
        self.invoke()
        self.client.messages[CHAT] = [message(2, chat=OTHER), message(3)]
        self.client.messages[CHAT][1].document.id += 1000
        self.assertEqual(json.loads(self.invoke())["skipped"], 3)
        self.assertEqual(self.client.downloads, [1, 2, 3])

    def test_flood_wait_blocks_reconnect_and_preserves_retry(self):
        self.client.messages[CHAT] = [message(1)]
        self.client.payload[1] = FloodWaitError(None, capture=70)
        with patch.object(sync.time, "time", return_value=1000):
            self.assertIn("70s", self.invoke(code=1))
            self.assertIn("1070", self.invoke(code=1))
            self.assertEqual(self.client.connects, 1)
        self.assertEqual(self.query("SELECT attempts,media_path FROM messages"), [(0, None)])
        self.client.payload.clear()
        self.assertEqual(json.loads(self.invoke())["downloaded"], 1)

    def test_reject_invalid_inputs_before_connect(self):
        self.invoke(since="2025-01-01T00:00:00", code=2)
        self.invoke(chat=" ", code=2)
        self.invoke(limit=0, code=2)
        self.assertFalse(list(self.output.iterdir()))
        self.assertEqual(self.client.connects, 0)

    def test_group_ttl_and_protected_media(self):
        rows = [message(i) for i in range(1, 6)]
        for row, ttl in zip(rows, (61, 60, 59, 120, 120), strict=True):
            row.ttl_period = ttl
        rows[3].media.ttl_seconds = 1
        rows[4].noforwards = True
        self.client.messages[CHAT] = rows
        with patch.object(sync.time, "time", return_value=START.timestamp() + 60):
            self.invoke()
        self.assertEqual(self.client.downloads, [1])
        self.assertEqual(self.query("SELECT msg_id FROM messages"), [(1,)])

    def test_expiry_rechecked_on_retry_and_before_publication(self):
        now = [START.timestamp()]
        first, second = message(1), message(2)
        first.ttl_period = second.ttl_period = 30
        self.client.messages[CHAT] = [first, second]
        self.client.payload[1] = OSError("connection lost")

        def advance_clock(msg):
            if msg.id == 2:
                now[0] += 30

        self.client.on_download = advance_clock
        with patch.object(sync.time, "time", side_effect=lambda: now[0]):
            self.invoke()
            self.invoke()
        self.assertEqual(self.client.downloads, [1, 2])
        self.assertEqual(self.client.fetches, [(CHAT, 1)])
        self.assertEqual(self.query("SELECT msg_id,media_path FROM messages ORDER BY msg_id"),
                         [(1, "skipped:expired"), (2, "skipped:expired")])
        self.assertFalse(list((self.output / "media" / str(CHAT)).iterdir()))

    def test_overlapping_download_keeps_db_writable_and_first_publication(self):
        self.client.messages[CHAT] = [message(1)]
        calls = []

        async def overlapping(msg, file, progress_callback):
            calls.append(file.name)
            if len(calls) == 1:
                with closing(sqlite3.connect(self.output / "messages.db", timeout=0)) as db, db:
                    self.assertEqual(db.execute("SELECT text FROM messages").fetchone(),
                                     ("caption 1",))
                    db.execute("INSERT INTO cursors VALUES(?, 'other scope', 9)", (OTHER,))
                other_counts = dict(scanned=0, downloaded=0, skipped=0, retry_pending=0, failed=0)
                await sync.run(str(CHAT), START, self.output, 1, other_counts)
                self.assertEqual(other_counts["downloaded"], 1)
                file.write(b"old")
            else:
                file.write(b"new")

        self.client.download_media = overlapping
        self.assertEqual(json.loads(self.invoke())["downloaded"], 0)
        self.assertEqual(len(set(calls)), 2)
        self.assertEqual(self.query(f"SELECT last_id FROM cursors WHERE chat_id={OTHER}"), [(9,)])
        folder = self.output / "media" / str(CHAT)
        self.assertEqual([p.read_bytes() for p in folder.iterdir()], [b"new"])

    def test_foreign_or_malformed_archive_rejected_unchanged(self):
        from tg_cli.db import MessageDB

        path = self.output / "messages.db"
        with MessageDB(path):
            pass

        def reject():
            before = path.read_bytes()
            self.assertIn("archive", self.invoke(code=1).lower())
            self.assertEqual(path.read_bytes(), before)

        reject()
        self.assertEqual(self.client.connects, 0)
        path.unlink()
        self.invoke()
        with closing(sqlite3.connect(path)) as db, db:
            app_id, = db.execute("PRAGMA application_id").fetchone()
            self.assertNotEqual(app_id, 0)
            db.execute("PRAGMA application_id=12345")
        reject()
        with closing(sqlite3.connect(path)) as db, db:
            db.execute(f"PRAGMA application_id={app_id}")
            db.execute("ALTER TABLE messages ADD COLUMN unrelated TEXT")
        reject()
        self.assertEqual(self.client.connects, 1)
