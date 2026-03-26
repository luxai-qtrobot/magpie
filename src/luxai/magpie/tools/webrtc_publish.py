#!/usr/bin/env python3
"""
magpie-publish-webrtc — publish messages over a WebRTC data channel.

Usage:
    magpie-publish-webrtc <session_id> <topic> <data> [options]
    magpie-publish-webrtc my-session /robot/state '{"x":1.0}' --signaling mqtt://127.0.0.1:1883
"""
import argparse
import ast
import json
import sys
import time
from typing import Any, Optional

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.frames import DictFrame
from luxai.magpie.tools._webrtc_tools_common import build_signaler


def _parse_payload(raw: str) -> Any:
    raw = raw.strip()
    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"payload file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f"invalid JSON in {path}: {e}")
    try:
        return ast.literal_eval(raw)
    except Exception:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-publish-webrtc",
        description="Publish messages to a Magpie WebRTC topic",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the subscriber (e.g. my-robot)")
    parser.add_argument("topic", type=str,
                        help="Topic to publish on (e.g. /robot/state)")
    parser.add_argument("data", type=_parse_payload,
                        help="Payload as JSON/Python literal or @file.json. Must be a dict unless --raw.")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only). "
                             "One peer must bind, the other connects.")
    parser.add_argument("--rate", type=float, default=None,
                        help="Publish rate in Hz. If omitted, publishes once and exits.")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of messages to publish (requires --rate).")
    parser.add_argument("--raw", action="store_true",
                        help="Publish payload as-is without DictFrame wrapping.")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    if not args.raw and not isinstance(args.data, dict):
        Logger.error("magpie-publish-webrtc: payload must be a dict when not using --raw")
        sys.exit(2)
    if args.rate is not None and args.rate <= 0:
        Logger.error("magpie-publish-webrtc: --rate must be > 0")
        sys.exit(2)

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-pub",
                              timeout=args.timeout, bind=args.bind)
    conn = WebRTCConnection(signaler=signaler)
    conn.connect()

    pub = WebRTCPublisher(conn)

    published = 0
    try:
        if args.rate is None:
            payload = args.data if args.raw else DictFrame(value=args.data).to_dict()
            pub.write(payload, topic=args.topic)
            published += 1
            Logger.info(f"magpie-publish-webrtc: published 1 message")
        else:
            frame_period = 1.0 / args.rate
            while True:
                t = time.time()
                payload = args.data if args.raw else DictFrame(value=args.data).to_dict()
                pub.write(payload, topic=args.topic)
                published += 1
                if args.count is not None and published >= args.count:
                    Logger.info(f"magpie-publish-webrtc: published {published} messages")
                    break
                elapsed = time.time() - t
                if elapsed < frame_period:
                    time.sleep(frame_period - elapsed)
    except KeyboardInterrupt:
        Logger.info(f"magpie-publish-webrtc: interrupted ({published} messages sent)")

    pub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
