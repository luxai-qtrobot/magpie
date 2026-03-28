#!/usr/bin/env python3
"""
magpie-request-webrtc — send a single RPC request over a WebRTC data channel.

Usage:
    magpie-request-webrtc <session_id> <service> [payload] [options]
    magpie-request-webrtc my-session /robot/motion '{"action":"move","x":1.0}' --signaling mqtt://127.0.0.1:1883
"""
import argparse
import ast
import json
import sys
from typing import Any, Dict, Optional

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcRequester  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.tools._webrtc_tools_common import build_signaler, webrtc_options_type, build_webrtc_options
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type


def _parse_payload(raw: str) -> Dict[str, Any]:
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
        data = ast.literal_eval(raw)
    except Exception:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(
                f"invalid payload — use JSON or Python dict literal. Error: {e}"
            )
    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("payload must be a dict.")
    return data


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-request-webrtc",
        description="Send a single RPC request over a Magpie WebRTC data channel",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the responder (e.g. my-robot)")
    parser.add_argument("service", type=str,
                        help="Service name (e.g. /robot/motion)")
    parser.add_argument("payload", nargs="?", type=_parse_payload, default={},
                        help="Request payload as JSON/Python dict or @file.json (default: {}).")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("--call-timeout", type=float, default=None,
                        metavar="SECS",
                        help="RPC call timeout in seconds. If omitted, waits forever.")
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        metavar="JSON|@FILE",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=webrtc_options_type, default=None,
                        metavar="JSON|@FILE",
                        help="WebRTC options (TURN servers, codecs, …) as JSON or @file.json.")
    parser.add_argument("--pretty", action="store_true",
                        help="Pretty-print JSON response.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-req",
                              timeout=args.timeout, bind=args.bind,
                              mqtt_params=args.mqtt_params)
    conn = WebRTCConnection(signaler=signaler, reconnect=True,
                            options=build_webrtc_options(args.webrtc_options))
    client: Optional[WebRTCRpcRequester] = None
    try:
        conn.connect()
        client = WebRTCRpcRequester(conn, service_name=args.service)
        response = client.call(args.payload, timeout=args.call_timeout)

        if args.pretty:
            print(json.dumps(response, indent=2, ensure_ascii=False))
        else:
            print(response)

    except TimeoutError:
        Logger.warning("magpie-request-webrtc: RPC timed out")
        sys.exit(2)
    except KeyboardInterrupt:
        Logger.info("magpie-request-webrtc: interrupted")
        sys.exit(130)
    except Exception as e:
        Logger.error(f"magpie-request-webrtc: {e}")
        sys.exit(1)
    finally:
        if client is not None:
            client.close()
        conn.disconnect()


if __name__ == "__main__":
    main()
