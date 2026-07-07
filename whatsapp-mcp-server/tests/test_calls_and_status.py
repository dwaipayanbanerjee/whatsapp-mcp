"""Tests for the list_calls and get_bridge_status tools."""

import sqlite3

import pytest

import whatsapp


@pytest.fixture
def calls_db(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE chats (jid TEXT PRIMARY KEY, name TEXT, last_message_time TIMESTAMP);
        CREATE TABLE messages (
            id TEXT, chat_jid TEXT, sender TEXT, content TEXT, timestamp TIMESTAMP,
            is_from_me BOOLEAN, media_type TEXT, filename TEXT,
            deleted_at TIMESTAMP, quoted_message_id TEXT,
            PRIMARY KEY (id, chat_jid)
        );
        CREATE TABLE calls (
            call_id TEXT, chat_jid TEXT, from_jid TEXT, timestamp TIMESTAMP,
            is_from_me BOOLEAN, call_type TEXT, is_group BOOLEAN, result TEXT,
            duration_sec INTEGER, ended_at TIMESTAMP, reason TEXT,
            PRIMARY KEY (call_id, chat_jid)
        );
        INSERT INTO chats VALUES ('1234567890@s.whatsapp.net', 'Alice', '2024-01-15 12:00:00+00:00');
        INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me)
        VALUES ('m1', '1234567890@s.whatsapp.net', '1234567890', 'hi', '2024-01-15 12:00:00+00:00', 0);
        INSERT INTO calls VALUES
            ('call-1', '1234567890@s.whatsapp.net', '1234567890@s.whatsapp.net',
             '2024-01-15 10:00:00+00:00', 0, 'voice', 0, 'answered', 90,
             '2024-01-15 10:01:30+00:00', ''),
            ('call-2', '1234567890@s.whatsapp.net', '1234567890@s.whatsapp.net',
             '2024-01-15 11:00:00+00:00', 0, 'video', 0, 'missed', NULL,
             '2024-01-15 11:00:30+00:00', 'timeout'),
            ('call-3', 'group-1@g.us', '9876543210@s.whatsapp.net',
             '2024-01-16 09:00:00+00:00', 0, 'voice', 1, 'rejected', NULL,
             NULL, '');
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(tmp_path / "missing-whatsapp.db"))
    whatsapp._sender_name_cache.clear()
    return db_path


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="OK"):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"status": "ok", "connected": True}
        self.text = text

    def json(self):
        return self._payload


def test_list_calls_returns_newest_first(calls_db):
    calls = whatsapp.list_calls()

    assert [c["call_id"] for c in calls] == ["call-3", "call-2", "call-1"]
    answered = calls[2]
    assert answered["result"] == "answered"
    assert answered["duration_sec"] == 90
    assert answered["from_name"] == "Alice"
    assert answered["is_group"] is False
    assert calls[0]["is_group"] is True


def test_list_calls_filters_by_type_and_date(calls_db):
    video_only = whatsapp.list_calls(call_type="video")
    assert [c["call_id"] for c in video_only] == ["call-2"]

    recent = whatsapp.list_calls(after="2024-01-16")
    assert [c["call_id"] for c in recent] == ["call-3"]


def test_list_calls_pagination(calls_db):
    page0 = whatsapp.list_calls(limit=2, page=0)
    page1 = whatsapp.list_calls(limit=2, page=1)

    assert [c["call_id"] for c in page0] == ["call-3", "call-2"]
    assert [c["call_id"] for c in page1] == ["call-1"]


def test_list_calls_rejects_bad_inputs(calls_db):
    with pytest.raises(ValueError, match="Invalid date format"):
        whatsapp.list_calls(after="not-a-date")
    with pytest.raises(ValueError, match="Invalid call_type"):
        whatsapp.list_calls(call_type="carrier-pigeon")


def test_list_calls_missing_table_gives_clear_error(tmp_path, monkeypatch):
    db_path = tmp_path / "messages.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE chats (jid TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(db_path))

    with pytest.raises(RuntimeError, match="restart the whatsapp-bridge"):
        whatsapp.list_calls()


def test_bridge_status_connected(calls_db, monkeypatch):
    monkeypatch.setattr(whatsapp.requests, "get", lambda *a, **kw: DummyResponse())

    status = whatsapp.get_bridge_status()

    assert status["bridge_reachable"] is True
    assert status["whatsapp_connected"] is True
    assert status["messages_db_exists"] is True
    assert status["message_count"] == 1
    assert status["chat_count"] == 1
    assert status["last_message_time"] == "2024-01-15T12:00:00+00:00"


def test_bridge_status_disconnected_503(calls_db, monkeypatch):
    resp = DummyResponse(status_code=503, payload={"status": "disconnected", "connected": False})
    monkeypatch.setattr(whatsapp.requests, "get", lambda *a, **kw: resp)

    status = whatsapp.get_bridge_status()

    assert status["bridge_reachable"] is True
    assert status["whatsapp_connected"] is False


def test_bridge_status_unauthorized_explains_token(calls_db, monkeypatch):
    resp = DummyResponse(status_code=401, payload={}, text="Unauthorized")
    monkeypatch.setattr(whatsapp.requests, "get", lambda *a, **kw: resp)

    status = whatsapp.get_bridge_status()

    assert status["bridge_reachable"] is True
    assert "WHATSAPP_BRIDGE_TOKEN" in status["bridge_error"]


def test_bridge_status_bridge_down(calls_db, monkeypatch):
    def raise_connection_error(*a, **kw):
        raise whatsapp.requests.ConnectionError("refused")

    monkeypatch.setattr(whatsapp.requests, "get", raise_connection_error)

    status = whatsapp.get_bridge_status()

    assert status["bridge_reachable"] is False
    assert "Bridge unreachable" in status["bridge_error"]
    # DB stats still work — reads never depend on the bridge being up.
    assert status["message_count"] == 1


def test_bridge_status_missing_db(tmp_path, monkeypatch):
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(tmp_path / "missing.db"))
    monkeypatch.setattr(
        whatsapp.requests, "get", lambda *a, **kw: (_ for _ in ()).throw(whatsapp.requests.ConnectionError("refused"))
    )

    status = whatsapp.get_bridge_status()

    assert status["messages_db_exists"] is False
    assert "db_error" in status
