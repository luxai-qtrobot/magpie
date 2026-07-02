#!/usr/bin/env python3
"""Shared helpers for WebRTC CLI tools."""
import argparse
import json
import sys
from typing import Optional

from luxai.magpie.utils.logger import Logger


# ---------------------------------------------------------------------------
# --webrtc-options parser
# ---------------------------------------------------------------------------

def webrtc_options_type(raw: str) -> dict:
    """
    argparse type for ``--webrtc-options``.

    Accepts either:
      - A path prefixed with ``@``: ``@opts.json``
      - An inline JSON object: ``{"video_bitrate": 4000}``
    """
    raw = raw.strip()
    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"webrtc-options file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(
                f"invalid JSON in webrtc-options file '{path}': {e}"
            )
    else:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(
                f"invalid JSON for --webrtc-options: {e}. "
                "Use an inline JSON object or @path/to/file.json"
            )
    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("--webrtc-options must be a JSON object")
    return data


def build_webrtc_options(d: Optional[dict], signaling_url: str = ""):
    """
    Convert a parsed ``--webrtc-options`` dict into a ``WebRTCOptions`` instance.

    Supported keys (all optional)::

        {
            "stun_servers":                ["stun:stun.l.google.com:19302"],
            "turn_servers": [
                {"url": "turn:host:3478", "username": "u", "credential": "p"}
            ],
            "ice_transport_policy":        "all",
            "data_channel_ordered":        true,
            "data_channel_max_retransmits": null,
            "video_codec":                 "H264",
            "audio_codec":                 "opus",
            "video_bitrate":               2000,
            "audio_bitrate":               96,
            "use_media_channels":          false
        }
    """
    from luxai.magpie.transport.webrtc import WebRTCOptions, WebRTCTurnServer  # noqa: PLC0415

    if not d:
        # ZMQ signaling is local/LAN — no need for STUN
        scheme = signaling_url.split("://")[0].lower() if "://" in signaling_url else ""
        if scheme == "tcp":
            from luxai.magpie.transport.webrtc import WebRTCOptions  # noqa: PLC0415
            return WebRTCOptions(stun_servers=[])
        return None

    turn_servers = []
    for t in d.get("turn_servers", []):
        turn_servers.append(WebRTCTurnServer(
            url=t["url"],
            username=t.get("username"),
            credential=t.get("credential"),
        ))

    kwargs = {}
    if "stun_servers" in d:
        kwargs["stun_servers"] = d["stun_servers"]
    if turn_servers:
        kwargs["turn_servers"] = turn_servers
    if "ice_transport_policy" in d:
        kwargs["ice_transport_policy"] = d["ice_transport_policy"]
    if "data_channel_ordered" in d:
        kwargs["data_channel_ordered"] = d["data_channel_ordered"]
    if "data_channel_max_retransmits" in d:
        kwargs["data_channel_max_retransmits"] = d["data_channel_max_retransmits"]
    if "video_codec" in d:
        kwargs["video_codec"] = d["video_codec"]
    if "audio_codec" in d:
        kwargs["audio_codec"] = d["audio_codec"]
    if "video_bitrate" in d:
        kwargs["video_bitrate"] = d["video_bitrate"]
    if "audio_bitrate" in d:
        kwargs["audio_bitrate"] = d["audio_bitrate"]
    if "use_media_channels" in d:
        kwargs["use_media_channels"] = d["use_media_channels"]

    return WebRTCOptions(**kwargs)


# ---------------------------------------------------------------------------
# Signaler factory
# ---------------------------------------------------------------------------

def build_signaler(signaling_url: str, session_id: str,
                   client_id: str = None, timeout: float = 10.0,
                   bind: bool = False, mqtt_params: dict = None):
    """
    Parse *signaling_url* and return a connected :class:`WebRtcSignaler`.

    Supported schemes:
      ``mqtt://``  ``mqtts://``  — MQTT broker  (requires ``luxai-magpie[mqtt]``)
      ``tcp://``                 — ZMQ PAIR socket  (included in base install)

    Args:
        signaling_url: Signaling URL, e.g. ``mqtt://127.0.0.1:1883``
                       or ``tcp://192.168.1.10:5555``.
        session_id:    Shared rendezvous name.
        client_id:     Optional MQTT client ID (ignored for ZMQ).
        timeout:       MQTT broker connection timeout in seconds (ignored for ZMQ).
        bind:          For ZMQ: ``True`` to bind the socket (server side).
        mqtt_params:   Parsed ``--mqtt-params`` dict for auth/TLS options (MQTT only).

    Returns the connected signaler.  Calls ``sys.exit(1)`` on failure.
    """
    from luxai.magpie.transport.webrtc import MqttSignaler, ZmqSignaler  # noqa: PLC0415

    scheme = signaling_url.split("://")[0].lower() if "://" in signaling_url else ""

    if scheme in ("mqtt", "mqtts", "ws", "wss"):
        try:
            from luxai.magpie.tools._mqtt_tools_common import build_mqtt_options, get_mqtt_protocol_version  # noqa: PLC0415
            mqtt_options = build_mqtt_options(mqtt_params)
            signaler = MqttSignaler(
                signaling_url, session_id,
                client_id=client_id,
                timeout=timeout,
                options=mqtt_options,
                protocol_version=get_mqtt_protocol_version(mqtt_params),
            )
        except ImportError:
            Logger.error(
                "MQTT signaling requires paho-mqtt. Install with:\n"
                "  pip install \"luxai-magpie[mqtt]\""
            )
            sys.exit(1)
        except ConnectionError as e:
            Logger.error(str(e))
            sys.exit(1)
        return signaler

    elif scheme == "tcp":
        try:
            signaler = ZmqSignaler(signaling_url, session_id, bind=bind)
        except ImportError as e:
            Logger.error(str(e))
            sys.exit(1)
        return signaler

    else:
        Logger.error(
            f"Unsupported signaling scheme '{scheme}://'. "
            "Supported: mqtt://, mqtts://, tcp:// (ZMQ)"
        )
        sys.exit(1)
