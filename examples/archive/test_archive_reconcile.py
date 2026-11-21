import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import archive_reconcile as ar


class ArchiveTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.scope = ar.Scope(42, 1)
        self.media = self.root / "media"
        self.peer_dir = self.media / str(self.scope.peers[1])
        self.peer_dir.mkdir(parents=True)
        self.tg = self.root / "messages.db"
        self.anti = self.root / "antidelete.db"
        self.out = self.root / "state/media-era.sqlite3"
        self.pos, self.neg = self.scope.peers
        self.sql(
            self.tg,
            """CREATE TABLE messages(platform,chat_id INTEGER,msg_id INTEGER,
                 timestamp,content,raw_json,UNIQUE(platform,chat_id,msg_id))""",
        )
        self.sql(
            self.anti,
            """CREATE TABLE messages(chat_id INTEGER,msg_id INTEGER,date,text,
                 media_type,media_path,PRIMARY KEY(chat_id,msg_id))""",
        )

    def sql(self, path, query, args=()):
        with closing(sqlite3.connect(path)) as db, db:
            return db.execute(query, args).fetchall()

    def cache(self, mid, body, raw=None, chat=None, platform="telegram"):
        self.sql(
            self.tg,
            "INSERT INTO messages VALUES(?,?,?,'t',?,?)",
            (platform, chat or self.pos, mid, body, raw),
        )

    def bot(self, mid, body, kind=None, path=None, chat=None):
        self.sql(
            self.anti,
            "INSERT INTO messages VALUES(?,?,'t',?,?,?)",
            (chat or self.neg, mid, body, kind, path),
        )

    def run_sync(self, **kwargs):
        return ar.reconcile(
            self.tg, self.anti, self.out, self.scope, self.root, self.media, batch_size=2, **kwargs
        )

    def test_independent_union_and_exact_scope(self):
        self.cache(1, "  sample\n", '{"media_type":"photo"}')
        self.cache(2, None, "invalid json")
        self.bot(2, "")
        self.bot(3, "bot-only")
        self.cache(4, "irrelevant", chat=-self.pos)
        self.bot(5, "irrelevant", chat=self.neg - 1)
        self.cache(6, "irrelevant", platform="other")
        self.cache(2, "alt encoding", chat=self.neg)
        before = [
            self.sql(p, "SELECT * FROM messages ORDER BY chat_id,msg_id")
            for p in (self.tg, self.anti)
        ]
        self.assertEqual(self.run_sync()["observations_changed"], 5)
        self.assertEqual(self.run_sync(full=True)["observations_changed"], 0)
        self.assertEqual(
            before,
            [
                self.sql(p, "SELECT * FROM messages ORDER BY chat_id,msg_id")
                for p in (self.tg, self.anti)
            ],
        )
        self.tg.unlink()
        self.anti.unlink()
        self.assertEqual(
            ar.status(self.out, self.scope),
            dict(
                messages=3,
                tg_cli_only=1,
                antidelete_only=1,
                both_sources=1,
                differing_texts=1,
                with_local_media=0,
            ),
        )
        rows = self.sql(
            self.out,
            """SELECT source_chat_id,text,media_state FROM observations
                                   WHERE msg_id<=2 ORDER BY msg_id,source,source_chat_id""",
        )
        self.assertEqual(
            rows,
            [
                (self.pos, "  sample\n", "unavailable"),
                (self.neg, "", "none_recorded"),
                (self.neg, "alt encoding", "none_recorded"),
                (self.pos, None, "none_recorded"),
            ],
        )
        self.assertEqual(self.sql(self.out, "SELECT DISTINCT chat_id FROM messages"), [(self.pos,)])

    def test_rotation_during_growth_and_late_media(self):
        for mid in (10, 20, 30, 40, 50, 60):
            self.bot(mid, str(mid), "photo")
        self.cache(5, "early")
        self.run_sync()
        self.run_sync()
        self.bot(5, "late")
        self.sql(
            self.anti,
            "UPDATE messages SET media_path=? WHERE msg_id=10",
            (str((self.peer_dir / "late.jpg").relative_to(self.root)),),
        )
        (self.peer_dir / "late.jpg").write_bytes(b"fixture")
        for n in range(5):
            for i in range(3):
                self.bot(61 + 3 * n + i, "growth")
            counts = self.run_sync()
            self.assertLessEqual(counts["source_rows_read"], 2 * 2 + 1)
        self.assertEqual(ar.status(self.out, self.scope)["both_sources"], 1)
        self.assertEqual(
            self.sql(self.out, "SELECT media_path,media_state FROM observations WHERE msg_id=10"),
            [(str((self.peer_dir / "late.jpg").relative_to(self.root)), "present")],
        )
        (self.peer_dir / "late.jpg").unlink()
        self.run_sync(full=True)
        self.assertEqual(ar.status(self.out, self.scope)["with_local_media"], 0)
        self.assertEqual(ar.status(self.out, self.scope)["messages"], 22)

    def test_media_states_and_original_values(self):
        (self.peer_dir / "empty").touch()
        (self.peer_dir / "photo").write_bytes(b"data")
        samples = [
            (None, None, "none_recorded"),
            ("photo", None, "unavailable"),
            ("photo", "None", "unavailable"),
            ("photo", "skipped:too_large", "skipped"),
            ("photo", "error:timeout", "error"),
            ("photo", str(self.peer_dir / "missing.jpg"), "missing"),
            ("photo", str(self.peer_dir / "empty"), "empty_file"),
            ("photo", str(self.peer_dir), "not_file"),
            ("photo", str(self.peer_dir / "photo"), "present"),
        ]
        for mid, (kind, path, _) in enumerate(samples, 1):
            self.bot(mid, "", kind, path)
        self.run_sync()
        self.assertEqual(
            self.sql(
                self.out,
                "SELECT media_type,media_path,media_state FROM observations ORDER BY msg_id",
            ),
            samples,
        )

    def test_rollback_and_retry(self):
        self.cache(1, "first")
        self.run_sync()
        progress = self.sql(self.out, "SELECT * FROM progress")
        self.cache(2, "new")
        self.bot(2, "new")
        self.sql(
            self.out,
            """CREATE TRIGGER fail BEFORE INSERT ON observations
                 WHEN NEW.source='antidelete' BEGIN SELECT raise(ABORT,'interrupted'); END""",
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.run_sync()
        self.assertEqual(self.sql(self.out, "SELECT * FROM progress"), progress)
        self.assertEqual(ar.status(self.out, self.scope)["messages"], 1)
        self.sql(self.out, "DROP TRIGGER fail")
        self.assertEqual(self.run_sync()["observations_changed"], 2)
        self.assertEqual(ar.status(self.out, self.scope)["both_sources"], 1)
        self.assertEqual(self.run_sync()["observations_changed"], 0)

    def test_scope_conflicts_and_independent_cursors(self):
        other = ar.Scope(43, 1)
        later = ar.Scope(self.pos, 2)
        self.bot(1, "one")
        self.bot(2, "two")
        self.bot(1, "other", chat=other.chat_id)
        self.run_sync()
        for scope, expected in [(other, "other"), (later, "two")]:
            before = self.out.read_bytes()
            with self.assertRaises(ValueError):
                ar.reconcile(self.tg, self.anti, self.out, scope, self.root, self.media)
            with self.assertRaises(ValueError):
                ar.status(self.out, scope)
            self.assertEqual(self.out.read_bytes(), before)
            out = self.root / scope.subdir / "archive.sqlite3"
            ar.reconcile(self.tg, self.anti, out, scope, self.root, self.media)
            self.assertEqual(self.sql(out, "SELECT text FROM observations"), [(expected,)])
        self.assertEqual(ar.status(self.out, ar.Scope(self.neg, 1))["messages"], 2)

    def test_live_wal_read_transaction_during_reconcile(self):
        self.run_sync()  # Exercise the incremental path.
        insert = "INSERT INTO messages VALUES('telegram',?,?,'t','fixture',NULL)"
        with closing(sqlite3.connect(self.tg, timeout=0)) as writer:
            self.assertEqual(writer.execute("PRAGMA journal_mode=WAL").fetchone(), ("wal",))
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute(insert, (self.pos, 5))
            writer.commit()
            writer.execute(insert, (self.neg, 6))  # Still uncommitted when the read starts.
            sync = ar.sync_stream

            def interleave(out, db, source, base, roots, chat, *args):
                self.assertEqual(db.execute("PRAGMA query_only").fetchone(), (1,))
                main_path = db.execute("PRAGMA database_list").fetchone()[2]
                self.assertEqual(Path(main_path), self.tg if source == "tg_cli" else self.anti)
                self.assertTrue(db.in_transaction)
                sync(out, db, source, base, roots, chat, *args)
                if source == "tg_cli" and chat == self.pos:
                    # The next stream reads the second encoding in the same transaction.
                    writer.commit()

            with patch.object(ar, "sync_stream", side_effect=interleave):
                self.assertEqual(self.run_sync()["observations_changed"], 1)
            self.assertEqual(self.sql(self.out, "SELECT msg_id FROM observations"), [(5,)])
            self.assertEqual(self.run_sync()["observations_changed"], 1)
            self.assertEqual(
                self.sql(self.out, "SELECT msg_id FROM observations ORDER BY msg_id"), [(5,), (6,)]
            )
            self.assertEqual(
                self.sql(self.tg, "SELECT msg_id FROM messages ORDER BY msg_id"), [(5,), (6,)]
            )
            self.assertEqual(self.sql(self.anti, "SELECT * FROM messages"), [])

    def test_cli_rejects_source_and_sidecar_outputs(self):
        self.cache(1, "private fixture")
        with closing(sqlite3.connect(self.tg)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("SELECT * FROM messages").fetchall()
            before = list(writer.iterdump())
            for path in [
                self.tg,
                self.anti,
                Path(str(self.tg) + "-wal"),
                Path(str(self.tg) + "-shm"),
                self.tg.with_name(self.tg.name.upper()),
                Path(str(self.anti) + "-journal") / "index",
            ]:
                result = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        ar.__file__,
                        "--chat-id",
                        str(self.pos),
                        "--first-msg-id",
                        "1",
                        "--tg-db",
                        str(self.tg),
                        "--anti-db",
                        str(self.anti),
                        "--tga-home",
                        str(self.root),
                        "--output",
                        str(path),
                    ],
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertNotIn(str(self.root), result.stderr)
                self.assertNotIn("private fixture", result.stderr)
            self.assertEqual(list(writer.iterdump()), before)

    def test_replaced_source_and_media_output_rejected(self):
        self.run_sync()
        self.anti.rename(self.root / "previous")
        self.anti.write_bytes((self.root / "previous").read_bytes())
        with self.assertRaises(ValueError):
            self.run_sync()
        with self.assertRaises(ValueError):
            ar.reconcile(
                self.tg, self.anti, self.peer_dir / "db", self.scope, self.root, self.media
            )
        self.assertFalse((self.peer_dir / "db").exists())

    def test_cli_and_output_guard(self):
        self.bot(10 - 1, "excluded fixture")
        self.bot(10, "private fixture")
        script = str(Path(ar.__file__))
        cmd = [
            sys.executable,
            "-B",
            script,
            "--tg-db",
            str(self.tg),
            "--chat-id",
            "42",
            "--first-msg-id",
            "10",
            "--output",
            str(self.out),
        ]
        env = dict(os.environ, TGA_HOME=str(self.root))
        run = subprocess.run(cmd, env=env, text=True, capture_output=True, check=True)
        self.assertEqual(json.loads(run.stdout)["observations_changed"], 1)
        self.assertNotIn("private fixture", run.stdout + run.stderr)
        self.assertEqual(self.out.stat().st_mode & 0o077, 0)
        run = subprocess.run(
            cmd + ["--status"], env=env, text=True, capture_output=True, check=True
        )
        self.assertEqual(json.loads(run.stdout)["antidelete_only"], 1)
        link = self.root / "alias"
        os.link(self.anti, link)
        with self.assertRaises(ValueError):
            ar.reconcile(self.tg, self.anti, link, self.scope, self.root, self.media)
        with self.assertRaises(FileNotFoundError):
            ar.reconcile(
                self.root / "missing.db", self.anti, self.out, self.scope, self.root, self.media
            )
        self.assertFalse((self.root / "missing.db").exists())


if __name__ == "__main__":
    unittest.main()
