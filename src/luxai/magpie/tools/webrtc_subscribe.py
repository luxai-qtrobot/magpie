#!/usr/bin/env python3
"""
magpie-subscribe-webrtc — subscribe to a WebRTC data channel topic.

Usage:
    magpie-subscribe-webrtc <session_id> <topic> [options]
    magpie-subscribe-webrtc my-session /robot/state --signaling mqtt://127.0.0.1:1883
"""
import argparse
import json
import sys
import time
from collections import deque
from time import perf_counter

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCSubscriber  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.tools._webrtc_tools_common import build_signaler


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-subscribe-webrtc",
        description="Subscribe to a Magpie WebRTC topic and print received messages",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the publisher (e.g. my-robot)")
    parser.add_argument("topic", type=str,
                        help="Topic to subscribe to (e.g. /robot/state)")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("--pretty", action="store_true",
                        help="Pretty-print JSON messages.")
    parser.add_argument("--once", action="store_true",
                        help="Receive one message and exit.")
    parser.add_argument("--hz", action="store_true",
                        help="Show message frequency (Hz) instead of message content.")
    parser.add_argument("--hz-window", type=int, default=100,
                        help="Sliding window size for Hz estimation (default: 100).")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-sub",
                              timeout=args.timeout, bind=args.bind)
    conn = WebRTCConnection(signaler=signaler, reconnect=True)
    conn.connect()

    sub = WebRTCSubscriber(conn, topic=args.topic)

    msg_count = 0
    ts_window = deque(maxlen=max(2, args.hz_window))
    last_hz_print = perf_counter()

    Logger.info(f"magpie-subscribe-webrtc: subscribed to '{args.topic}' on session '{args.session_id}'")

    try:
        while True:
            try:
                data, _ = sub.read(timeout=1.0)
            except TimeoutError:
                continue

            msg_count += 1

            if args.hz:
                now = perf_counter()
                ts_window.append(now)
                if (now - last_hz_print) >= 1.0:
                    hz = (len(ts_window) - 1) / (ts_window[-1] - ts_window[0]) if len(ts_window) >= 2 else 0.0
                    Logger.info(f"rate: {hz:.2f} Hz (msgs={msg_count})")
                    last_hz_print = now
            else:
                try:
                    if args.pretty:
                        print(json.dumps(data, indent=2, ensure_ascii=False))
                    else:
                        print(json.dumps(data, separators=(",", ":"), ensure_ascii=False))
                except (TypeError, ValueError):
                    print(data)

            if args.once:
                Logger.info("magpie-subscribe-webrtc: received one message (--once). Exiting.")
                break

    except KeyboardInterrupt:
        Logger.info("magpie-subscribe-webrtc: interrupted.")

    sub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
