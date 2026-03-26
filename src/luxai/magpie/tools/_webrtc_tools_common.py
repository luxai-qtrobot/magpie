#!/usr/bin/env python3
"""Shared helpers for WebRTC CLI tools."""
import sys
from luxai.magpie.utils.logger import Logger


def build_signaler(signaling_url: str, session_id: str,
                   client_id: str = None, timeout: float = 10.0,
                   bind: bool = False):
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

    Returns the connected signaler.  Calls ``sys.exit(1)`` on failure.
    """
    from luxai.magpie.transport.webrtc import MqttSignaler, ZmqSignaler  # noqa: PLC0415

    scheme = signaling_url.split("://")[0].lower() if "://" in signaling_url else ""

    if scheme in ("mqtt", "mqtts", "ws", "wss"):
        try:
            signaler = MqttSignaler(
                signaling_url, session_id,
                client_id=client_id,
                timeout=timeout,
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
