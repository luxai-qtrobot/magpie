#!/usr/bin/env python3
import argparse
import json
import sys
import time
from collections import deque
from time import perf_counter

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

from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import MqttConnection, MqttStreamReader
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options


class MqttSubscribe(SinkNode):
    """
    Magpie MQTT topic reader tool.

    Prints received messages to stdout.
    Supports:
      --pretty   Pretty JSON output
      --once     Exit after first message
      --hz       Show message frequency (Hz)
    """

    def setup(self, pretty=False, once=False, show_hz=False, hz_window=100):
        self.pretty = pretty
        self.once = once
        self.show_hz = show_hz

        self.msg_count = 0

        self.ts_window = deque(maxlen=max(2, hz_window))
        self.last_stats_print = perf_counter()

        Logger.info(f"{self.name}: subscribing")

    def process(self):
        _data = self.stream_reader.read(timeout=None)
        if _data is None:
            return

        data, _topic = _data
        self.msg_count += 1

        if self.show_hz:
            now = perf_counter()
            self.ts_window.append(now)

            if (now - self.last_stats_print) >= 1.0:
                hz = self._compute_hz()
                Logger.info(f"rate: {hz:.2f} Hz (msgs={self.msg_count})")
                self.last_stats_print = now
        else:
            try:
                if self.pretty:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(json.dumps(data, separators=(",", ":"), ensure_ascii=False))
            except (TypeError, ValueError):
                print(data)

        if self.once:
            Logger.info("Received one message (--once). Exiting...")
            self.terminate()

    def _compute_hz(self) -> float:
        if len(self.ts_window) < 2:
            return 0.0
        dt = self.ts_window[-1] - self.ts_window[0]
        if dt <= 0:
            return 0.0
        return (len(self.ts_window) - 1) / dt


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="magpie-read-mqtt",
        description="Subscribe to a Magpie MQTT topic and print received messages",
    )

    p.add_argument(
        "uri",
        type=str,
        help="MQTT broker URI (e.g. mqtt://broker:1883 or mqtts://broker:8883)",
    )
    p.add_argument(
        "topic",
        type=str,
        help="MQTT topic or wildcard pattern (e.g. robot/sensors/imu or robot/sensors/+)",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON messages",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Receive one message and exit",
    )
    p.add_argument(
        "--hz",
        action="store_true",
        help="Show message frequency (Hz)",
    )
    p.add_argument(
        "--hz-window",
        type=int,
        default=100,
        help="Sliding window size for Hz estimation (default: 100)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Connection timeout in seconds (default: 10).",
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


def main():
    parser = build_parser()
    ns = parser.parse_args()

    Logger.set_level("DEBUG" if ns.verbose else "INFO")

    opts = build_mqtt_options(ns.mqtt_params)

    conn = MqttConnection(ns.uri, options=opts)
    if not conn.connect(timeout=ns.timeout):
        Logger.error(f"magpie-read-mqtt: could not connect to broker at {ns.uri}")
        return 1

    subscriber = MqttStreamReader(conn, topic=ns.topic)

    node = MqttSubscribe(
        name="MagpieMqttSubscribe",
        stream_reader=reader,
        setup_kwargs={
            "pretty": ns.pretty,
            "once": ns.once,
            "show_hz": ns.hz,
            "hz_window": ns.hz_window,
        },
    )

    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break

    Logger.info("Closing...")
    node.terminate()
    reader.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
