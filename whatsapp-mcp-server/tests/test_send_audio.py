"""Tests for send_audio_message conversion placement.

The bridge only reads outbound media from its allowed roots
(WHATSAPP_MEDIA_ROOTS). Converting into the system temp directory therefore
got every non-.ogg voice message rejected with 403 — conversion must target
the outbox instead, and clean up after itself.
"""

import os

import whatsapp


class DummyResponse:
    status_code = 200

    def json(self):
        return {"success": True, "message": "sent"}


def test_send_audio_converts_into_outbox_and_cleans_up(monkeypatch, tmp_path):
    outbox = tmp_path / "outbox"
    monkeypatch.setenv("WHATSAPP_MEDIA_ROOTS", str(outbox))
    monkeypatch.setenv("WHATSAPP_BRIDGE_TOKEN", "test-token")

    src = tmp_path / "note.mp3"
    src.write_bytes(b"mp3-bytes")

    def fake_convert(input_file, output_file, *args, **kwargs):
        with open(output_file, "wb") as fh:
            fh.write(b"ogg-bytes")
        return output_file

    monkeypatch.setattr(whatsapp.audio, "convert_to_opus_ogg", fake_convert)

    sent_payloads = []

    def fake_post(url, json, headers=None, timeout=None):
        # The converted file must still exist at send time.
        assert os.path.isfile(json["media_path"])
        sent_payloads.append(json)
        return DummyResponse()

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    success, _ = whatsapp.send_audio_message("12025551234", str(src))

    assert success is True
    sent_path = sent_payloads[0]["media_path"]
    assert sent_path.startswith(str(outbox) + os.sep)
    assert sent_path.endswith(".ogg")
    # The converted copy is removed once the bridge call returns.
    assert not os.path.exists(sent_path)
    # The caller's original file is untouched.
    assert src.exists()


def test_send_audio_ogg_passes_through_unconverted(monkeypatch, tmp_path):
    monkeypatch.setenv("WHATSAPP_BRIDGE_TOKEN", "test-token")

    src = tmp_path / "voice.ogg"
    src.write_bytes(b"ogg-bytes")

    def fail_convert(*args, **kwargs):
        raise AssertionError("must not convert .ogg input")

    monkeypatch.setattr(whatsapp.audio, "convert_to_opus_ogg", fail_convert)

    sent_payloads = []

    def fake_post(url, json, headers=None, timeout=None):
        sent_payloads.append(json)
        return DummyResponse()

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    success, _ = whatsapp.send_audio_message("12025551234", str(src))

    assert success is True
    assert sent_payloads[0]["media_path"] == str(src)
    assert src.exists()


def test_send_audio_conversion_failure_reports_ffmpeg_hint(monkeypatch, tmp_path):
    monkeypatch.setenv("WHATSAPP_MEDIA_ROOTS", str(tmp_path / "outbox"))

    src = tmp_path / "note.mp3"
    src.write_bytes(b"mp3-bytes")

    def fail_convert(*args, **kwargs):
        raise RuntimeError("ffmpeg missing")

    monkeypatch.setattr(whatsapp.audio, "convert_to_opus_ogg", fail_convert)

    success, message = whatsapp.send_audio_message("12025551234", str(src))

    assert success is False
    assert "ffmpeg" in message


def test_outbox_dir_uses_first_media_root(monkeypatch):
    monkeypatch.setenv("WHATSAPP_MEDIA_ROOTS", f"/first/root{os.pathsep}/second/root")
    assert whatsapp._outbox_dir() == "/first/root"


def test_outbox_dir_defaults_to_xdg_outbox(monkeypatch):
    monkeypatch.delenv("WHATSAPP_MEDIA_ROOTS", raising=False)
    assert whatsapp._outbox_dir().endswith(os.path.join(".local", "share", "whatsapp-mcp", "outbox"))
