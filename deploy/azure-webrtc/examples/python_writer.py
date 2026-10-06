"""Publish test values through the deployed signaling and TURN services."""

import argparse
import time

from common import fetch_webrtc_options
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter
from luxai.magpie.utils import Logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, help="Container App base URL")
    parser.add_argument("--session", required=True, help="Shared unique session ID")
    parser.add_argument("--topic", default="demo/messages")
    parser.add_argument("--force-turn", action="store_true")
    args = parser.parse_args()

    options = fetch_webrtc_options(args.server, args.session, args.force_turn)
    connection = WebRTCConnection.with_http(
        args.server.rstrip("/") + "/signal",
        session_id=args.session,
        role="host",
        options=options,
    )

    Logger.info(f"Waiting for a client in session {args.session!r} ...")
    connect_started = time.monotonic()
    if not connection.connect(timeout=60):
        Logger.error("No peer connected within 60 seconds")
        raise SystemExit("No peer connected within 60 seconds")
    Logger.info(f"Connected in {time.monotonic() - connect_started:.2f} seconds")

    writer = WebRtcStreamWriter(connection)
    try:
        for sequence in range(1, 11):
            value = {"sequence": sequence, "message": "Hello from MAGPIE Python"}
            writer.write(value, topic=args.topic)
            Logger.info(f"sent: {value}")
            time.sleep(1)
    finally:
        writer.close()
        connection.disconnect()


if __name__ == "__main__":
    main()
