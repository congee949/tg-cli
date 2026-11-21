"""Tests for Telegram client helpers without hitting the network."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner
from telethon import types
from telethon.errors import FloodWaitError

from tg_cli.cli import _sync, tg
from tg_cli.cli.main import cli
from tg_cli.client import fetch_history, sync_all
from tg_cli.db import MessageDB


@dataclass
class FakeEntity:
    id: int
    title: str


@dataclass
class FakeDialog:
    entity: FakeEntity
    name: str


@dataclass
class FakeSender:
    id: int
    first_name: str = "User"
    last_name: str = ""
    username: str | None = None


@dataclass
class FakeMessage:
    id: int
    sender_id: int
    text: str
    date: datetime
    message: str | None = None
    _sender: object = None

    def __post_init__(self):
        if self._sender is None:
            self._sender = FakeSender(id=self.sender_id)


class FakeClient:
    def __init__(self, dialogs: list[FakeDialog], messages_by_chat: dict[int, list[FakeMessage]]):
        self._dialogs = dialogs
        self._messages_by_chat = messages_by_chat

    async def get_entity(self, chat):
        if isinstance(chat, FakeEntity):
            return chat
        for dialog in self._dialogs:
            if chat == dialog.entity.id or chat == dialog.name:
                return dialog.entity
        raise ValueError(f"unknown chat: {chat}")

    async def iter_dialogs(self):
        for dialog in self._dialogs:
            yield dialog

    async def iter_messages(self, entity, limit: int, min_id: int = 0, reverse: bool = False):
        messages = sorted(
            (msg for msg in self._messages_by_chat[entity.id] if msg.id > min_id),
            key=lambda msg: msg.id,
            reverse=not reverse,
        )
        for msg in messages[:limit]:
            yield msg


@pytest.mark.asyncio
async def test_fetch_history_returns_inserted_count(db):
    entity = FakeEntity(id=100, title="Test Group")
    client = FakeClient(
        dialogs=[FakeDialog(entity=entity, name="Test Group")],
        messages_by_chat={
            100: [
                FakeMessage(id=1, sender_id=1, text="old", date=datetime.now(timezone.utc)),
                FakeMessage(id=2, sender_id=1, text="new-1", date=datetime.now(timezone.utc)),
                FakeMessage(id=3, sender_id=1, text="new-2", date=datetime.now(timezone.utc)),
            ]
        },
    )

    db.insert_message(
        chat_id=100,
        chat_name="Test Group",
        msg_id=1,
        sender_id=1,
        sender_name="Alice",
        content="old",
        timestamp=datetime.now(timezone.utc),
    )

    inserted = await fetch_history(client, 100, db=db, limit=10, batch_delay=0)
    assert inserted == 2


@pytest.mark.asyncio
async def test_sync_all_discovers_dialogs_from_client(db):
    dialogs = [
        FakeDialog(entity=FakeEntity(id=100, title="Group A"), name="Group A"),
        FakeDialog(entity=FakeEntity(id=200, title="Group B"), name="Group B"),
    ]
    client = FakeClient(
        dialogs=dialogs,
        messages_by_chat={
            100: [FakeMessage(id=1, sender_id=1, text="hello", date=datetime.now(timezone.utc))],
            200: [FakeMessage(id=1, sender_id=2, text="world", date=datetime.now(timezone.utc))],
        },
    )

    results = await sync_all(client, db, limit_per_chat=10, delay=0)
    assert results == {"Group A": 1, "Group B": 1}
    assert db.count() == 2


@pytest.mark.asyncio
async def test_sync_all_max_chats_limits_synced_dialogs(db):
    dialogs = [
        FakeDialog(entity=FakeEntity(id=100, title="Group A"), name="Group A"),
        FakeDialog(entity=FakeEntity(id=200, title="Group B"), name="Group B"),
        FakeDialog(entity=FakeEntity(id=300, title="Group C"), name="Group C"),
    ]
    client = FakeClient(
        dialogs=dialogs,
        messages_by_chat={
            100: [FakeMessage(id=1, sender_id=1, text="hello", date=datetime.now(timezone.utc))],
            200: [FakeMessage(id=1, sender_id=2, text="world", date=datetime.now(timezone.utc))],
            300: [FakeMessage(id=1, sender_id=3, text="bye", date=datetime.now(timezone.utc))],
        },
    )

    results = await sync_all(client, db, limit_per_chat=10, delay=0, max_chats=1)
    assert len(results) == 1
    assert db.count() == 1


@pytest.mark.asyncio
async def test_connect_uses_default_credentials_when_env_unset(monkeypatch):
    """When TG_API_ID/TG_API_HASH are not set, connect() should use Telegram Desktop defaults."""
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)

    from tg_cli.config import get_api_hash, get_api_id

    api_id = get_api_id()
    api_hash = get_api_hash()
    # Defaults should be set (Telegram Desktop credentials)
    assert api_id is not None
    assert api_hash is not None
    assert isinstance(api_id, int)
    assert len(api_hash) > 0


PEER = "-1000000000017"
NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


class ArchiveClient(FakeClient):
    def __init__(self):
        entity = FakeEntity(17, "Live")
        super().__init__(
            [FakeDialog(entity, entity.title)],
            {17: [FakeMessage(i, 1, str(i), NOW) for i in range(1, 9)]},
        )
        self.connections = 0
        self.connected = True

    async def get_entity(self, chat):
        if chat == "dropped":
            self.connected = False
            raise RuntimeError("lost transport")
        if chat == "offline":
            raise ConnectionError("offline")
        if chat == "busy":
            raise FloodWaitError(request=None, capture=23)
        if chat == "fetch-wait":
            return FakeEntity(21, "Busy fetch")
        if chat in (int(PEER), "Alias"):
            return self._dialogs[0].entity
        return await super().get_entity(chat)

    def is_connected(self):
        return self.connected

    async def iter_messages(self, entity, **kwargs):
        if entity.id == 21:
            raise FloodWaitError(request=None, capture=19)
        async for msg in super().iter_messages(entity, **kwargs):
            yield msg


@pytest.fixture
def archive(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "messages.db"))
    client = ArchiveClient()

    @asynccontextmanager
    async def connect():
        client.connections += 1
        yield client

    monkeypatch.setattr(_sync, "connect", connect)
    monkeypatch.setattr(tg, "connect", connect)
    return client


def invoke(*args):
    result = CliRunner().invoke(cli, list(args), catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)["data"]


def seed(chat_id, name, msg_id, platform="telegram"):
    with MessageDB() as db:
        db.insert_batch(
            [dict(chat_id=chat_id, chat_name=name, msg_id=msg_id, timestamp=NOW)],
            platform=platform,
        )


def stored_ids():
    with MessageDB() as db:
        rows = db.conn.execute(
            "SELECT msg_id FROM messages WHERE platform='telegram' AND chat_id=17 ORDER BY msg_id"
        )
        return [row[0] for row in rows]


@pytest.mark.parametrize("command", ["sync", "sync-many", "sync-all"])
def test_incremental_drains_backlog_in_order(archive, command):
    seed(17, "Old title", 1)
    seed(17, "Old title", 900, platform="example")
    seed(22, "Alias", 800)
    args = {
        "sync": ["sync", "--json", "--limit", "3", "Alias"],
        "sync-many": ["sync-many", "--json", "--request=Alias=3"],
        "sync-all": ["sync-all", "--json", "--limit", "3", "--delay", "0"],
    }[command]
    for last in [4, 7, 8, 8]:
        invoke(*args)
        assert stored_ids() == list(range(1, last + 1))


@pytest.mark.parametrize("command", ["sync", "history", "sync-many"])
def test_first_page_is_latest(archive, command):
    seed(17, "Live", 900, platform="example")
    if command == "history":
        seed(17, "Live", 1)
    if command == "sync-many":
        data = invoke(command, "--json", f"--request={PEER}=2")
        (row,) = data["results"]
        assert row["chat"] == PEER and row["resolved_chat_id"] == 17
        assert type(row["synced"]) is int and row["synced"] == 2
    else:
        invoke(command, "--json", "--limit", "2", "Alias")
    assert stored_ids() == ([1, 7, 8] if command == "history" else [7, 8])


def test_batch_is_ordered_and_reports_each_outcome(archive):
    seed(17, "Live", 1)
    seed(31, "Ambiguous One", 99)
    seed(32, "Ambiguous Two", 98)
    peers = [PEER, "busy", "fetch-wait", "missing", "Ambiguous", PEER]
    rows = invoke("sync-many", "--json", *(f"--request={peer}=2" for peer in peers))["results"]
    assert archive.connections == 1
    assert [row["chat"] for row in rows] == peers
    assert [
        (r["status"], r["synced"], r["resolved_chat_id"], r["retry_after_seconds"]) for r in rows
    ] == [
        ("ok", 2, 17, None),
        ("rate_limited", 0, None, 23),
        ("rate_limited", 0, 21, 19),
        ("failed", 0, None, None),
        ("failed", 0, None, None),
        ("ok", 2, 17, None),
    ]
    assert stored_ids() == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("peer", ["offline", "dropped"])
def test_batch_connection_failure_has_nonzero_exit(archive, peer):
    result = CliRunner().invoke(cli, ["sync-many", "--json", f"--request={peer}=1"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["ok"] is False


@pytest.mark.parametrize("request_arg", ["bad", "=2", "Alias=-1", "Alias=two"])
def test_invalid_request_fails_before_connect(archive, request_arg):
    result = CliRunner().invoke(cli, ["sync-many", "--json", f"--request={request_arg}"])
    assert result.exit_code == 2
    assert archive.connections == 0


def service_event(i):
    return types.MessageService(
        id=i, peer_id=types.PeerChat(17), date=NOW, action=types.MessageActionPinMessage()
    )


@pytest.mark.parametrize("command", ["sync", "sync-many", "history"])
def test_service_page_and_uncaptioned_media(archive, command):
    seed(17, "Live", 1)
    archive._messages_by_chat[17] = [
        service_event(2),
        service_event(3),
        service_event(4),
        types.Message(
            id=5, peer_id=types.PeerChat(17), date=NOW, message="", media=types.MessageMediaPhoto()
        ),
        service_event(6),
    ]
    if command == "sync-many":
        invoke(command, "--json", f"--request={PEER}=2")
    else:
        invoke(command, "--json", "--limit", "2", "Live")
    assert stored_ids() == [1, 5]
    with MessageDB() as db:
        row = db.conn.execute(
            "SELECT content, json_extract(raw_json, '$.media_type') "
            "FROM messages WHERE chat_id=17 AND msg_id=5"
        ).fetchone()
        assert tuple(row) == ("", "MessageMediaPhoto")
    if command != "history":
        assert invoke("sync", "--json", "--limit", "2", "Live")["synced"] == 0


def test_history_charges_service_events_to_page_limit(archive):
    archive._messages_by_chat[17].extend([service_event(9), service_event(10)])
    assert invoke("history", "--json", "--limit", "2", "Live")["stored"] == 0
    assert stored_ids() == []


def test_sync_resumes_at_first_unread_id(archive):
    seed(17, "Live", 2)
    assert invoke("sync", "--json", "--limit", "2", "Live")["synced"] == 2
    assert stored_ids() == [2, 3, 4]


def test_batch_connect_failure(archive, monkeypatch):
    @asynccontextmanager
    async def connect():
        raise ConnectionError("connection failed")
        yield

    monkeypatch.setattr(_sync, "connect", connect)
    result = CliRunner().invoke(cli, ["sync-many", "--json", f"--request={PEER}=2"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"]["code"] == "sync_many_failed"
