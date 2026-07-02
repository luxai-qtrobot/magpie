#!/usr/bin/env python3
import argparse
import ast
import json
import sys
import time
from typing import Any, Optional

try:
    import paho.mqtt  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Could not import paho-mqtt. Please install it with:\n"
        "  pip install \"luxai-magpie[mqtt]\"\n"
        "  or: pip install paho-mqtt"
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.nodes.source_node import SourceNode
from luxai.magpie.transport import MqttConnection, MqttStreamWriter
from luxai.magpie.frames import DictFrame
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options, get_mqtt_protocol_version


def _parse_payload(raw: str) -> Any:
    """
    Parse payload input. Supports:
      - Any JSON/Python literal: string, number, list, dict
      - Python-literal (PowerShell-friendly): "{'name':'Bob'}" or "'hello'"
      - JSON string: '{"name":"Bob"}' or '"hello"'
      - JSON file: '@payload.json'
      - Plain string fallback (when --raw is used)
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
            except json.JSONDecodeError:
                data = raw  # treat as plain string

    return data


class MqttWrite(SourceNode):

    def setup(self, topic: str, data: Any,
              rate: Optional[float] = None,
              count: Optional[int] = None,
              loop: bool = False,
              raw: bool = False):
        self.topic = topic
        self.data = data
        self.rate = rate
        self.count = count
        self.loop = loop
        self.raw = raw
        self._written = 0
        self._write_time = None

        Logger.info(f"{self.name}: topic={self.topic} rate={self.rate}Hz "
                    f"count={self.count} loop={self.loop} raw={self.raw}")

    def process(self):
        self._write_time = time.time()

        payload = self.data if self.raw else DictFrame(value=self.data).to_dict()

        # Single-shot: write once then exit
        if self.rate is None:
            if self._written == 0:
                self.stream_writer.write(payload, topic=self.topic)
                self._written += 1
                Logger.info(f"{self.name}: wrote 1 message")
            else:
                time.sleep(0.5)
                self.terminate()
            return

        # Rate mode: write then sleep to maintain rate
        if self.count is None or self._written < self.count:
            self.stream_writer.write(payload, topic=self.topic)
            self._written += 1

            if self.count is not None and self._written >= self.count:
                Logger.info(f"{self.name}: wrote {self._written} messages (waiting for Ctrl+C)")
        else:
            time.sleep(0.5)
            self.terminate()
            return

        # Ensure frame rate
        time_diff = time.time() - self._write_time
        frame_period = 1.0 / float(self.rate)
        if time_diff < frame_period:
            time.sleep(frame_period - time_diff)


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-write-mqtt",
        description="Write a message to a Magpie MQTT topic",
    )

    parser.add_argument(
        "uri",
        type=str,
        help="MQTT broker URI (e.g. mqtt://broker:1883 or mqtts://broker:8883)",
    )
    parser.add_argument(
        "topic",
        type=str,
        help="MQTT topic to publish to (e.g. robot/sensors/imu)",
    )
    parser.add_argument(
        "data",
        type=_parse_payload,
        help="Payload as any JSON/Python literal or @file.json. Must be a dict unless --raw is set.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Write rate in Hz. If omitted, writes once and exits.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of messages to write (requires --rate). If omitted with --rate: write forever.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Write forever (requires --rate).",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Write payload as-is without wrapping in DictFrame. Allows any type, not just dict.",
    )
    parser.add_argument(
        "--retain",
        action="store_true",
        help="Set the MQTT retain flag on published messages.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Connection timeout in seconds (default: 10).",
    )
    parser.add_argument(
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
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )

    args = parser.parse_args()

    Logger.set_level("DEBUG" if args.verbose else "INFO")

    if not args.raw and not isinstance(args.data, dict):
        Logger.error("magpie-write-mqtt: payload must be a dict when not using --raw")
        return 2
    if args.rate is not None and args.rate <= 0:
        Logger.error("magpie-write-mqtt: --rate must be > 0")
        return 2
    if args.loop and args.rate is None:
        Logger.error("magpie-write-mqtt: --loop requires --rate")
        return 2
    if args.count is not None and args.count <= 0:
        Logger.info("magpie-write-mqtt: --count <= 0, nothing to do")
        return 0

    opts = build_mqtt_options(args.mqtt_params)
    # --retain flag overrides whatever is in --mqtt-params
    if args.retain:
        opts.defaults.publish_retain = True

    conn = MqttConnection(args.uri, options=opts, protocol_version=get_mqtt_protocol_version(args.mqtt_params))
    if not conn.connect(timeout=args.timeout):
        Logger.error(f"magpie-write-mqtt: could not connect to broker at {args.uri}")
        return 1

    writer = MqttStreamWriter(conn)

    node = MqttWrite(
        name="MagpieMqttWrite",
        stream_writer=writer,
        setup_kwargs={
            "topic": args.topic,
            "data": args.data,
            "rate": args.rate,
            "count": args.count,
            "loop": args.loop,
            "raw": args.raw,
        },
    )

    while not node.terminating():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break

    Logger.info("Closing...")
    node.terminate()
    writer.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
