"""Tests for read-only database access and sender-name caching.

The MCP server must never create or write the bridge's databases: a plain
sqlite3.connect() silently creates an empty file when the configured path is
wrong, which surfaces later as a confusing "no such table" error instead of
a clear "database not found" one.
"""

import sqlite3

import pytest

import whatsapp


@pytest.fixture(autouse=True)
def clear_sender_name_cache():
    whatsapp._sender_name_cache.clear()
    yield
    whatsapp._sender_name_cache.clear()


def _make_messages_db(path):
    conn = sqlite3.connect(path)
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
        INSERT INTO chats VALUES ('1234567890@s.whatsapp.net', 'Alice', '2024-01-15 10:30:00+00:00');
        """
    )
    conn.commit()
    conn.close()


def test_missing_messages_db_raises_clear_error(tmp_path, monkeypatch):
    missing = tmp_path / "nope" / "messages.db"
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(missing))

    with pytest.raises(FileNotFoundError, match="messages.db not found"):
        whatsapp.list_messages(limit=1)

    # The failed attempt must not have created an empty database file.
    assert not missing.exists()


def test_messages_db_opened_read_only(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    _make_messages_db(str(db_path))
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))

    conn = whatsapp._connect_messages_db()
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO chats (jid) VALUES ('x@s.whatsapp.net')")
    finally:
        conn.close()


def test_missing_whatsmeow_db_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "whatsapp.db"))
    assert whatsapp._connect_whatsmeow_db() is None


def test_get_sender_name_survives_missing_db(tmp_path, monkeypatch):
    """Name resolution is decoration; a missing DB must not fail the read."""
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(tmp_path / "missing.db"))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "missing2.db"))

    assert whatsapp.get_sender_name("999@s.whatsapp.net") == "999@s.whatsapp.net"


def test_get_sender_name_uses_cache(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    _make_messages_db(str(db_path))
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "missing.db"))

    lookups = []
    real_lookup = whatsapp._lookup_sender_name

    def counting_lookup(jid):
        lookups.append(jid)
        return real_lookup(jid)

    monkeypatch.setattr(whatsapp, "_lookup_sender_name", counting_lookup)

    first = whatsapp.get_sender_name("1234567890@s.whatsapp.net")
    second = whatsapp.get_sender_name("1234567890@s.whatsapp.net")

    assert first == second == "Alice"
    assert len(lookups) == 1


def test_sender_name_cache_expires(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    _make_messages_db(str(db_path))
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "missing.db"))

    lookups = []
    monkeypatch.setattr(whatsapp, "_lookup_sender_name", lambda jid: lookups.append(jid) or "Alice")

    now = [1000.0]
    monkeypatch.setattr(whatsapp.time, "monotonic", lambda: now[0])

    whatsapp.get_sender_name("1234567890@s.whatsapp.net")
    now[0] += whatsapp._SENDER_NAME_TTL_SECONDS + 1
    whatsapp.get_sender_name("1234567890@s.whatsapp.net")

    assert len(lookups) == 2
