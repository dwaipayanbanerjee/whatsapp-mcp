"""Tests for the deleted-state and media-filename fields on message dicts.

The bridge stamps deleted_at when a sender uses "delete for everyone" and
stores document filenames, but neither was surfaced to MCP clients — an
agent couldn't tell a retracted message from a live one.
"""

import sqlite3
from datetime import datetime

import pytest

import whatsapp
from whatsapp import Message, msg_to_dict


@pytest.fixture
def messages_db(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE chats (jid TEXT PRIMARY KEY, name TEXT, last_message_time TIMESTAMP);
        CREATE TABLE messages (
            id TEXT, chat_jid TEXT, sender TEXT, content TEXT, timestamp TIMESTAMP,
            is_from_me BOOLEAN, media_type TEXT, filename TEXT, url TEXT,
            media_key BLOB, file_sha256 BLOB, file_enc_sha256 BLOB, file_length INTEGER,
            deleted_at TIMESTAMP, quoted_message_id TEXT,
            PRIMARY KEY (id, chat_jid)
        );
        INSERT INTO chats VALUES ('1234567890@s.whatsapp.net', 'Alice', '2024-01-15 10:32:00+00:00');
        INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, deleted_at)
        VALUES
            ('msg-live', '1234567890@s.whatsapp.net', '1234567890', 'still here',
             '2024-01-15 10:30:00+00:00', 0, NULL, NULL, NULL),
            ('msg-deleted', '1234567890@s.whatsapp.net', '1234567890', 'oops',
             '2024-01-15 10:31:00+00:00', 0, NULL, NULL, '2024-01-15 10:35:00+00:00'),
            ('msg-doc', '1234567890@s.whatsapp.net', '1234567890', 'the report',
             '2024-01-15 10:32:00+00:00', 0, 'document', 'q3-report.pdf', NULL);
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "missing-whatsapp.db"))
    whatsapp._sender_name_cache.clear()
    return db_path


def _by_id(messages):
    return {m["id"]: m for m in messages}


def test_list_messages_surfaces_deleted_state(messages_db):
    msgs = _by_id(whatsapp.list_messages(limit=10, include_context=False))

    assert msgs["msg-live"]["is_deleted"] is False
    assert msgs["msg-live"]["deleted_at"] is None
    assert msgs["msg-deleted"]["is_deleted"] is True
    assert msgs["msg-deleted"]["deleted_at"] == "2024-01-15T10:35:00+00:00"
    # Content is preserved for the archive even after retraction.
    assert msgs["msg-deleted"]["content"] == "oops"


def test_list_messages_surfaces_media_filename(messages_db):
    msgs = _by_id(whatsapp.list_messages(limit=10, include_context=False))

    assert msgs["msg-doc"]["media_filename"] == "q3-report.pdf"
    assert msgs["msg-doc"]["reaction_to_message_id"] is None
    assert msgs["msg-live"]["media_filename"] is None


def test_get_message_context_carries_deleted_state(messages_db):
    context = whatsapp.get_message_context("msg-deleted", before=1, after=1)

    assert context.message.deleted_at is not None
    assert msg_to_dict(context.message, include_sender_name=False)["is_deleted"] is True
    surrounding = [msg_to_dict(m, include_sender_name=False) for m in context.before + context.after]
    assert all(m["is_deleted"] is False for m in surrounding)


def test_get_last_interaction_includes_new_fields(messages_db):
    result = whatsapp.get_last_interaction("1234567890@s.whatsapp.net")

    assert result["id"] == "msg-doc"
    assert result["media_filename"] == "q3-report.pdf"
    assert result["is_deleted"] is False


def test_reaction_filename_not_exposed_as_media_filename():
    reaction = Message(
        id="react-1",
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        sender="1234567890@s.whatsapp.net",
        content="👍",
        is_from_me=False,
        chat_jid="1234567890@s.whatsapp.net",
        media_type="reaction",
        filename="target-msg-id",
    )

    d = msg_to_dict(reaction, include_sender_name=False)

    assert d["reaction_to_message_id"] == "target-msg-id"
    assert d["media_filename"] is None
