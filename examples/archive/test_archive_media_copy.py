import errno
import json
import os
import sqlite3
import subprocess
import sys
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import archive_media_copy as m


@unittest.skipUnless(sys.platform == "darwin", "requires macOS clonefile")
class MediaCopyTest(unittest.TestCase):
    scope = m.Scope(42, 10)

    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.home = Path(temp.name).resolve()
        self.root = self.home / "media" / str(self.scope.peers[1])
        self.root.mkdir(parents=True)
        self.dest = self.home / "media-backup" / self.scope.subdir
        self.index = self.dest / "manifest.sqlite3"
        self.source = self.home / "antidelete.db"
        self.mid = self.scope.first_msg_id
        self.sql(
            self.source,
            """CREATE TABLE messages(chat_id INTEGER,msg_id INTEGER,
                 media_path TEXT,PRIMARY KEY(chat_id,msg_id))""",
        )

    def sql(self, db_path, sql, args=()):
        with closing(sqlite3.connect(db_path)) as db, db:
            return db.execute(sql, args).fetchall()

    def file(self, offset, ext="jpg", data=b"fixture"):
        path = self.root / f"{self.mid + offset}.{ext}"
        path.write_bytes(data)
        return path

    def record(self, offset, path, peer=None):
        self.sql(
            self.source,
            "INSERT INTO messages VALUES(?,?,?)",
            (peer or self.scope.peers[1], self.mid + offset, str(path)),
        )

    def run_at(self, t=100, **args):
        with patch.object(m.time, "time", return_value=t):
            return m.run(
                self.home,
                self.source,
                self.root.parent,
                self.home / "media-backup",
                args.pop("scope", self.scope),
                **args,
            )

    def test_real_clone_preference_boundaries_and_query(self):
        self.file(-1)
        self.record(-1, self.root / f"{self.mid - 1}.jpg")
        self.file(0)
        preferred = self.file(0, "png", b"verified fixture")
        self.record(0, preferred.relative_to(self.home), peer=self.scope.chat_id)
        orphan = self.file(1)
        self.file(2)
        self.file(2, "png")
        self.file(3, data=b"")
        self.record(3, self.root / f"{self.mid + 3}.jpg")
        self.record(4, self.root / f"{self.mid + 4}.jpg")
        self.file(5).unlink()
        (self.root / f"{self.mid + 5}.jpg").symlink_to(self.source)
        foreign = self.home / "media/123"
        foreign.mkdir()
        wrong = foreign / f"{self.mid + 6}.jpg"
        wrong.write_bytes(b"foreign")
        self.record(6, wrong)
        # A DB mapping is required for a non-numbered name in the target directory.
        generic = self.root / "attachment.bin"
        generic.write_bytes(b"data")
        self.record(7, generic)
        self.record(8, generic, peer=-123)
        src_before = self.sql(self.source, "SELECT * FROM messages ORDER BY chat_id,msg_id")
        first = self.run_at(init=True)
        self.assertEqual(first["cloned"], 3)
        rows = self.sql(self.index, "SELECT msg_id,source,backup_name FROM copies ORDER BY msg_id")
        self.assertEqual(
            [(mid - self.mid, source) for mid, source, name in rows],
            [(0, "db"), (1, "orphan"), (7, "db")],
        )
        dst = self.dest / rows[0][2]
        self.assertNotEqual(dst.stat().st_ino, preferred.stat().st_ino)
        self.assertEqual(dst.stat().st_dev, preferred.stat().st_dev)
        self.assertEqual(dst.read_bytes(), preferred.read_bytes())
        preferred.write_bytes(b"edited")
        self.assertEqual(dst.read_bytes(), b"verified fixture")
        orphan.unlink()
        self.assertEqual((self.dest / rows[1][2]).read_bytes(), b"fixture")
        self.assertEqual(
            self.sql(self.source, "SELECT * FROM messages ORDER BY chat_id,msg_id"), src_before
        )
        self.assertEqual(self.run_at(400)["cloned"], 0)
        self.assertEqual(self.sql(self.index, "SELECT * FROM pending"), [])

    def test_periodic_budget_no_old_missing_retries_and_daily_orphans(self):
        self.record(0, self.root / "absent.bin")
        self.record(1, self.file(1, data=b""))
        self.run_at(init=True)
        self.assertEqual(self.sql(self.index, "SELECT * FROM pending"), [])
        self.file(4)
        for i in (5, 6, 7):
            self.record(i, self.file(i))
        with patch.object(m, "PAGE", 2), patch.object(m, "scandir", side_effect=AssertionError):
            r = self.run_at(400, limit=1)
            self.assertEqual((r["db_rows_scanned"], r["cloned"]), (2, 1))
            self.assertEqual(self.run_at(700, limit=1)["cloned"], 1)
            self.assertEqual(self.run_at(1000)["cloned"], 1)
            with patch.object(m, "verified", side_effect=AssertionError):
                idle = self.run_at(1300)
                self.assertEqual((idle["db_rows_scanned"], idle["names_scanned"]), (0, 0))
        self.file(1)  # Formerly empty, now genuinely exists.
        r = self.run_at(100 + m.DAY)
        self.assertEqual(r["cloned"], 2)
        self.assertEqual(self.sql(self.index, "SELECT count(*) FROM copies"), [(5,)])

    def test_queue_rechecks_disappeared_files_and_new_ambiguity(self):
        self.file(0)
        queued = self.file(1)
        self.record(1, queued)
        self.file(2)
        self.assertEqual(self.run_at(init=True, limit=1)["cloned"], 1)
        queued.unlink()
        self.file(2, "png")
        with patch.object(m, "scandir", side_effect=AssertionError):
            self.assertEqual(self.run_at(400)["unavailable"], 2)
            self.assertEqual(self.sql(self.index, "SELECT count(*) FROM pending"), [(0,)])
            self.assertEqual(self.run_at(700)["unavailable"], 0)
        self.assertEqual(self.run_at(100 + m.DAY)["cloned"], 0)

    def test_failure_retry_and_crash_after_clone(self):
        self.record(0, self.file(0))
        with patch.object(m, "clone_fd", side_effect=OSError(errno.EXDEV, "different volume")):
            self.assertEqual(self.run_at(init=True)["failure_EXDEV"], 1)
        self.assertEqual(list(self.dest.glob("*.jpg")), [])
        self.assertEqual(self.run_at(200)["cloned"], 0)
        actual = m.copy_one

        def interrupted(*args):
            actual(*args)
            raise RuntimeError("simulated interruption")

        with patch.object(m, "copy_one", side_effect=interrupted), self.assertRaises(RuntimeError):
            self.run_at(400)
        inode = next(self.dest.glob("*.jpg")).stat().st_ino
        self.assertEqual(self.sql(self.index, "SELECT count(*) FROM copies"), [(0,)])
        self.assertEqual(self.run_at(401)["verified_existing"], 1)
        self.assertEqual(self.sql(self.index, "SELECT inode FROM copies"), [(inode,)])
        self.assertEqual(self.run_at(701)["cloned"], 0)

    def test_collision_and_hardlink_are_never_overwritten_or_counted(self):
        src = self.file(0)
        self.file(1)
        self.dest.mkdir(parents=True)
        a = self.dest / f"{self.mid}.orphan.jpg"
        os.link(src, a)  # adversarial pre-existing destination
        b = self.dest / f"{self.mid + 1}.orphan.jpg"
        b.write_bytes(b"changed")
        self.assertEqual(self.run_at(init=True)["failure_CONFLICT"], 2)
        self.assertEqual(b.read_bytes(), b"changed")
        self.assertEqual(self.sql(self.index, "SELECT count(*) FROM copies"), [(0,)])
        self.assertEqual(src.stat().st_ino, a.stat().st_ino)

    def test_overlapping_ids_and_starts_have_separate_manifests(self):
        other = m.Scope(43, self.mid)
        later = m.Scope(self.scope.chat_id, self.mid + 1)
        other_dir = self.root.parent / str(other.peers[1])
        other_dir.mkdir()
        (other_dir / f"{self.mid}.jpg").write_bytes(b"second channel")
        self.file(0)
        self.file(1)
        self.assertEqual(self.run_at(init=True)["cloned"], 2)
        self.assertEqual(self.run_at(init=True, scope=other)["cloned"], 1)
        self.assertEqual(self.run_at(scope=later)["cloned"], 1)

        def copies(scope):
            dest = self.home / "media-backup" / scope.subdir
            return {
                mid: (dest / name).read_bytes()
                for mid, name in self.sql(
                    dest / "manifest.sqlite3", "SELECT msg_id,backup_name FROM copies"
                )
            }

        self.assertEqual(copies(other), {self.mid: b"second channel"})
        self.assertEqual(copies(later), {self.mid + 1: b"fixture"})
        self.assertEqual(copies(self.scope), {self.mid: b"fixture", self.mid + 1: b"fixture"})

    def test_external_media_directory_and_explicit_database(self):
        with TemporaryDirectory() as tmp:
            external = Path(tmp).resolve()
            root = external / str(self.scope.chat_id)
            root.mkdir()
            generic = root / "file.bin"
            generic.write_bytes(b"example")
            self.record(0, generic)
            source = self.source.rename(self.home / "input.sqlite")
            cmd = [
                sys.executable,
                "-B",
                m.__file__,
                "--init",
                "--chat-id",
                str(self.scope.peers[1]),
                "--first-msg-id",
                str(self.mid),
                "--anti-db",
                str(source),
                "--media-dir",
                str(external),
                "--tga-home",
                str(self.home),
                "--backup-dir",
                str(self.home / "output"),
            ]
            run = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.assertEqual(json.loads(run.stdout)["cloned"], 1)
            output = self.home / "output" / self.scope.subdir / f"{self.mid}.db.bin"
            self.assertEqual(output.read_bytes(), generic.read_bytes())

    def test_cli(self):
        self.file(0)
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                m.__file__,
                "--chat-id",
                "42",
                "--first-msg-id",
                "10",
                "--init",
                "--limit",
                "1",
            ],
            env=dict(os.environ, TGA_HOME=str(self.home)),
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout)["cloned"], 1)
        self.assertNotIn(str(self.root), result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
