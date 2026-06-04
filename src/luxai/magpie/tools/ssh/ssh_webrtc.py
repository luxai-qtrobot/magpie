#!/usr/bin/env python3
"""
magpie-ssh-webrtc — SSH client tunnel over WebRTC.

Two usage modes:

  1. Direct (wraps ssh automatically):
       magpie-ssh-webrtc --signaling mqtt://broker:1883 my-node
       magpie-ssh-webrtc --signaling mqtt://broker:1883 my-node ls -l

  2. ProxyCommand mode (called by ssh, used in ~/.ssh/config):
       magpie-ssh-webrtc --proxy --signaling mqtt://broker:1883 my-node

  ~/.ssh/config example:
       Host my-node
           ProxyCommand magpie-ssh-webrtc --proxy --signaling mqtt://broker:1883 %h
           User robot

  Then: ssh my-node  (or VS Code Remote, scp, rsync, …)
"""
import argparse
import os
import sys
import threading

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter, WebRtcStreamReader
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type
from luxai.magpie.tools._webrtc_tools_common import (
    webrtc_options_type, build_signaler, build_webrtc_options,
)
from luxai.magpie.tools.ssh._ssh_tools_common import (
    SSH_STREAM_TOPIC_UP, SSH_STREAM_TOPIC_DOWN,
    bridge_stdin_to_writer, bridge_reader_to_stdout,
    build_proxy_command,
)


def _run_proxy(node_id: str, signaling: str, mqtt_params: dict,
               webrtc_options: dict, timeout: float) -> None:
    """Proxy mode: bridge stdin/stdout ↔ WebRTC data channel (called by ssh as ProxyCommand)."""
    signaler = build_signaler(
        signaling, node_id,
        client_id="magpie-ssh-webrtc",
        timeout=timeout,
        bind=False,
        mqtt_params=mqtt_params,
    )
    conn = WebRTCConnection(
        signaler=signaler,
        reconnect=False,
        options=build_webrtc_options(webrtc_options, signaling),
    )

    if not conn.connect(timeout=timeout):
        Logger.error("magpie-ssh-webrtc: WebRTC handshake timed out")
        sys.exit(1)

    writer = WebRtcStreamWriter(connection=conn)
    reader = WebRtcStreamReader(connection=conn, topic=SSH_STREAM_TOPIC_DOWN)

    stop = threading.Event()

    t_in = threading.Thread(
        target=bridge_stdin_to_writer,
        args=(writer, SSH_STREAM_TOPIC_UP, stop),
        daemon=True,
        name="ssh-webrtc-stdin",
    )
    t_out = threading.Thread(
        target=bridge_reader_to_stdout,
        args=(reader, stop),
        daemon=True,
        name="ssh-webrtc-stdout",
    )
    t_in.start()
    t_out.start()

    stop.wait()

    reader.close()
    writer.close()
    conn.disconnect()


def _run_client(node_id: str, signaling: str, mqtt_params_raw: str,
                webrtc_options_raw: str, timeout: float, ssh_extra: list) -> None:
    """Client mode: exec into ssh with this tool as ProxyCommand."""
    extra_args = ["--signaling", signaling]
    if mqtt_params_raw:
        extra_args += ["--mqtt-params", mqtt_params_raw]
    if webrtc_options_raw:
        extra_args += ["--webrtc-options", webrtc_options_raw]
    if timeout != 30.0:
        extra_args += ["--timeout", str(timeout)]

    proxy_cmd = build_proxy_command("magpie-ssh-webrtc", node_id, extra_args)

    ssh_cmd = ["ssh", "-o", f"ProxyCommand={proxy_cmd}"] + ssh_extra + [node_id]
    Logger.debug(f"magpie-ssh-webrtc: exec: {' '.join(ssh_cmd)}")
    os.execvp("ssh", ssh_cmd)


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-ssh-webrtc",
        description="SSH tunnel over WebRTC. Without --proxy, wraps ssh automatically.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Direct use — interactive shell:
  magpie-ssh-webrtc --signaling mqtt://broker:1883 my-node

  # Direct use — run a command:
  magpie-ssh-webrtc --signaling mqtt://broker:1883 my-node ls -l

  # ProxyCommand for ~/.ssh/config:
  #   Host my-node
  #       ProxyCommand magpie-ssh-webrtc --proxy --signaling mqtt://broker:1883 %h
""",
    )
    parser.add_argument("node_id", type=str,
                        help="Target node identifier (matches server node_id)")
    parser.add_argument("--proxy", action="store_true",
                        help="Run as ProxyCommand: bridge stdin/stdout to WebRTC stream.")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or mqtts://host:port "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--mqtt-params", type=str, default=None,
                        dest="mqtt_params_raw", metavar="JSON|@FILE",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=str, default=None,
                        dest="webrtc_options_raw", metavar="JSON|@FILE",
                        help="WebRTC options (STUN/TURN servers, …) as JSON or @file.json.")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="WebRTC handshake timeout in seconds (default: 30)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args, ssh_extra = parser.parse_known_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    mqtt_params = None
    if args.mqtt_params_raw:
        mqtt_params = mqtt_params_type(args.mqtt_params_raw)

    webrtc_options = None
    if args.webrtc_options_raw:
        webrtc_options = webrtc_options_type(args.webrtc_options_raw)

    if args.proxy:
        _run_proxy(args.node_id, args.signaling, mqtt_params, webrtc_options, args.timeout)
    else:
        _run_client(args.node_id, args.signaling, args.mqtt_params_raw,
                    args.webrtc_options_raw, args.timeout, ssh_extra)


if __name__ == "__main__":
    main()
