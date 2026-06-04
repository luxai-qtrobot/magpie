#!/usr/bin/env python3
"""
magpie-ssh-mqtt — SSH client tunnel over MQTT.

Two usage modes:

  1. Direct (wraps ssh automatically):
       magpie-ssh-mqtt mqtt://broker:1883 my-node
       magpie-ssh-mqtt mqtt://broker:1883 my-node ls -l

  2. ProxyCommand mode (called by ssh, used in ~/.ssh/config):
       magpie-ssh-mqtt --proxy mqtt://broker:1883 my-node

  ~/.ssh/config example:
       Host my-node
           ProxyCommand magpie-ssh-mqtt --proxy mqtt://broker:1883 %h
           User robot

  Then: ssh my-node  (or VS Code Remote, scp, rsync, …)
"""
import argparse
import os
import sys
import threading

try:
    import paho.mqtt  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "MQTT transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[mqtt]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from luxai.magpie.transport import MqttConnection, MqttStreamWriter, MqttStreamReader
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options
from luxai.magpie.tools.ssh._ssh_tools_common import (
    mqtt_up_topic, mqtt_down_topic,
    bridge_stdin_to_writer, bridge_reader_to_stdout,
    build_proxy_command,
)


def _run_proxy(uri: str, node_id: str, mqtt_params: dict, timeout: float) -> None:
    """Proxy mode: bridge stdin/stdout ↔ MQTT stream (called by ssh as ProxyCommand)."""
    session_ulid = get_uinque_id()
    up   = mqtt_up_topic(node_id, session_ulid)
    down = mqtt_down_topic(node_id, session_ulid)

    opts = build_mqtt_options(mqtt_params)
    conn = MqttConnection(uri, options=opts)
    if not conn.connect(timeout=timeout):
        Logger.error(f"magpie-ssh-mqtt: cannot connect to broker at {uri}")
        sys.exit(1)

    # queue_size=0 → no drop-oldest queue, safe for binary SSH data
    writer = MqttStreamWriter(conn, queue_size=0)
    reader = MqttStreamReader(conn, topic=down, queue_size=0)

    stop = threading.Event()

    t_in = threading.Thread(
        target=bridge_stdin_to_writer,
        args=(writer, up, stop),
        daemon=True,
        name="ssh-mqtt-stdin",
    )
    t_out = threading.Thread(
        target=bridge_reader_to_stdout,
        args=(reader, stop),
        daemon=True,
        name="ssh-mqtt-stdout",
    )
    t_in.start()
    t_out.start()

    stop.wait()

    reader.close()
    writer.close()
    conn.disconnect()


def _run_client(uri: str, node_id: str, mqtt_params_raw: str,
                timeout: float, ssh_extra: list) -> None:
    """Client mode: exec into ssh with this tool as ProxyCommand."""
    extra_args = [uri]
    if mqtt_params_raw:
        extra_args += ["--mqtt-params", mqtt_params_raw]
    if timeout != 10.0:
        extra_args += ["--timeout", str(timeout)]

    proxy_cmd = build_proxy_command("magpie-ssh-mqtt", node_id, extra_args)

    ssh_cmd = ["ssh", "-o", f"ProxyCommand={proxy_cmd}"] + ssh_extra + [node_id]
    Logger.debug(f"magpie-ssh-mqtt: exec: {' '.join(ssh_cmd)}")
    os.execvp("ssh", ssh_cmd)


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-ssh-mqtt",
        description="SSH tunnel over MQTT. Without --proxy, wraps ssh automatically.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Direct use — interactive shell:
  magpie-ssh-mqtt mqtt://broker:1883 my-node

  # Direct use — run a command:
  magpie-ssh-mqtt mqtt://broker:1883 my-node ls -l

  # ProxyCommand for ~/.ssh/config:
  #   Host my-node
  #       ProxyCommand magpie-ssh-mqtt --proxy mqtt://broker:1883 %h
""",
    )
    parser.add_argument("uri", type=str,
                        help="MQTT broker URI (e.g. mqtt://broker:1883)")
    parser.add_argument("node_id", type=str,
                        help="Target node identifier (matches server --node_id)")
    parser.add_argument("--proxy", action="store_true",
                        help="Run as ProxyCommand: bridge stdin/stdout to MQTT stream.")
    parser.add_argument("--mqtt-params", type=str, default=None,
                        dest="mqtt_params_raw", metavar="JSON|@FILE",
                        help="MQTT connection options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Broker connection timeout in seconds (default: 10)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args, ssh_extra = parser.parse_known_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    mqtt_params = None
    if args.mqtt_params_raw:
        mqtt_params = mqtt_params_type(args.mqtt_params_raw)

    if args.proxy:
        _run_proxy(args.uri, args.node_id, mqtt_params, args.timeout)
    else:
        _run_client(args.uri, args.node_id, args.mqtt_params_raw, args.timeout, ssh_extra)


if __name__ == "__main__":
    main()
