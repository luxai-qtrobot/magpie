#!/usr/bin/env python3
import argparse
import ast
import json
import sys
from typing import Any, Dict, Optional

try:
    import paho.mqtt  # noqa: F401
except ImportError:
    from luxai.magpie.utils import Logger
    Logger.error(
        "Could not import paho-mqtt. Please install it with:\n"
        "  pip install \"luxai-magpie[mqtt]\"\n"
        "  or: pip install paho-mqtt"
    )
    sys.exit(1)

from luxai.magpie.transport import MqttConnection, MqttRpcRequester
from luxai.magpie.utils import Logger
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options


def _parse_payload(raw: str) -> Dict[str, Any]:
    """
    Parse the payload input. Supports:
      - Python-literal dict (PowerShell-friendly): "{'action':'move','x':1.0}"
      - JSON object string: '{"action":"move","x":1.0}'
      - JSON file: '@payload.json'
    """
    raw = raw.strip()

    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"payload file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f"invalid JSON in payload file {path}: {e}")
    else:
        try:
            data = ast.literal_eval(raw)
        except Exception:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                raise argparse.ArgumentTypeError(
                    f"invalid payload. Use JSON like '{{\"action\":\"move\"}}' "
                    f"or a Python dict literal like \"{{'action':'move'}}\". "
                    f"Error: {e}"
                )

    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("payload must be a JSON/dict object.")

    return data


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="magpie-request-mqtt",
        description="Generic Magpie MQTT RPC requester",
    )

    p.add_argument(
        "uri",
        type=str,
        help="MQTT broker URI (e.g. mqtt://broker:1883 or mqtts://broker:8883)",
    )
    p.add_argument(
        "service",
        type=str,
        help="Service name / topic prefix used to derive the RPC request topic (e.g. robot/motion)",
    )
    p.add_argument(
        "payload",
        nargs="?",
        type=_parse_payload,
        default={},
        help=(
            "Request payload as a JSON object string, Python dict literal, or @file.json "
            "(e.g. '{\"action\":\"move\",\"x\":1.0}' or @req.json). Defaults to empty dict."
        ),
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Total RPC call timeout in seconds (default: 5).",
    )
    p.add_argument(
        "--ack-timeout",
        type=float,
        default=2.0,
        dest="ack_timeout",
        help="Timeout in seconds to wait for the responder ACK (default: 2). Must be < --timeout.",
    )
    p.add_argument(
        "--connect-timeout",
        type=float,
        default=10.0,
        dest="connect_timeout",
        help="Broker connection timeout in seconds (default: 10).",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON response to stdout",
    )
    p.add_argument(
        "--mqtt-params",
        type=mqtt_params_type,
        default=None,
        dest="mqtt_params",
        metavar="@FILE.json",
        help=(
            "Advanced MQTT connection parameters as a JSON file (prefix path with @) "
            "or inline JSON object. Supports: defaults (publish_qos, subscribe_qos, "
            "publish_retain), auth, tls, session, reconnect, will."
        ),
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )

    return p


def main() -> int:
    parser = build_parser()
    ns = parser.parse_args()

    Logger.set_level("DEBUG" if ns.verbose else "INFO")

    opts = build_mqtt_options(ns.mqtt_params)

    conn: Optional[MqttConnection] = None
    client: Optional[MqttRpcRequester] = None

    try:
        conn = MqttConnection(ns.uri, options=opts)
        if not conn.connect(timeout=ns.connect_timeout):
            Logger.error(f"magpie-request-mqtt: could not connect to broker at {ns.uri}")
            return 1

        client = MqttRpcRequester(conn, service_name=ns.service, ack_timeout=ns.ack_timeout)

        ret = client.call(ns.payload, timeout=ns.timeout)

        if ns.pretty:
            print(json.dumps(ret, indent=2, ensure_ascii=False))
        else:
            print(ret)

        return 0

    except TimeoutError:
        Logger.warning("magpie-request-mqtt: timeout on call")
        return 2
    except KeyboardInterrupt:
        Logger.info("magpie-request-mqtt: interrupted by user")
        return 130
    except Exception as e:
        Logger.error(f"magpie-request-mqtt: error: {e}")
        return 1
    finally:
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
