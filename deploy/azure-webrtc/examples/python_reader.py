"""Receive test values through the deployed signaling and TURN services."""

import argparse
import time

from common import fetch_webrtc_options
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader
from luxai.magpie.utils import Logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, help="Container App base URL")
    parser.add_argument("--session", required=True, help="Shared unique session ID")
    parser.add_argument("--topic", default="demo/messages")
    parser.add_argument("--force-turn", action="store_true")
    args = parser.parse_args()

    Logger.set_level('DEBUG')
    options = fetch_webrtc_options(args.server, args.session, args.force_turn)
    connection = WebRTCConnection.with_http(
        args.server.rstrip("/") + "/signal",
        session_id=args.session,
        role="client",
        options=options,
    )

    Logger.info(f"Waiting for the host in session {args.session!r} ...")
    connect_started = time.monotonic()
    if not connection.connect(timeout=60):
        Logger.error("No peer connected within 60 seconds")
        raise SystemExit("No peer connected within 60 seconds")
    Logger.info(f"Connected in {time.monotonic() - connect_started:.2f} seconds")

    reader = WebRtcStreamReader(connection, topic=args.topic)
    received = 0
    try:
        while received < 10:
            try:
                value, topic = reader.read(timeout=15)
            except TimeoutError:
                Logger.warning("Timed out waiting for a message")
                continue
            received += 1
            Logger.info(f"received on {topic}: {value}")
    finally:
        reader.close()
        connection.disconnect()


if __name__ == "__main__":
    main()
