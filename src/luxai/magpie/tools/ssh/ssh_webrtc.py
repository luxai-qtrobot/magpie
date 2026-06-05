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


def _run_client(node_id: str, signaling: str, mqtt_params: dict | None,
                webrtc_options: dict | None, timeout: float, ssh_extra: list) -> None:
    """Client mode: run ssh with this tool as ProxyCommand."""
    import subprocess
    import json
    import tempfile
    import os
    from pathlib import Path

    extra_args = ["--signaling", signaling]
    tmpfiles = []

    if mqtt_params:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(mqtt_params, f)
            tmpfiles.append(f.name)
        extra_args += ["--mqtt-params", f"@{Path(tmpfiles[-1]).as_posix()}"]

    if webrtc_options:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(webrtc_options, f)
            tmpfiles.append(f.name)
        extra_args += ["--webrtc-options", f"@{Path(tmpfiles[-1]).as_posix()}"]

    if timeout != 30.0:
        extra_args += ["--timeout", str(timeout)]

    proxy_cmd = build_proxy_command("magpie-ssh-webrtc", node_id, extra_args)

    # Only append node_id as SSH destination when ssh_extra has no destination.
    # A destination is any non-flag argument (not starting with '-').
    has_destination = any(not a.startswith("-") for a in ssh_extra)
    destination = [] if has_destination else [node_id]
    ssh_cmd = ["ssh", "-o", f"ProxyCommand={proxy_cmd}"] + ssh_extra + destination
    Logger.debug(f"magpie-ssh-webrtc: running: {ssh_cmd}")
    try:
        sys.exit(subprocess.run(ssh_cmd).returncode)
    finally:
        for tmp in tmpfiles:
            try:
                os.unlink(tmp)
            except OSError:
                pass


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
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        dest="mqtt_params", metavar="@FILE.json",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=webrtc_options_type, default=None,
                        dest="webrtc_options", metavar="@FILE.json",
                        help="WebRTC options (STUN/TURN servers, …) as JSON or @file.json.")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="WebRTC handshake timeout in seconds (default: 30)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")
    parser.add_argument("ssh_args", nargs=argparse.REMAINDER, metavar="...",
                        help="SSH options and/or destination (user@host) passed to ssh.")

    args = parser.parse_args()
    ssh_extra = args.ssh_args
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    if args.proxy:
        _run_proxy(args.node_id, args.signaling, args.mqtt_params, args.webrtc_options, args.timeout)
    else:
        _run_client(args.node_id, args.signaling, args.mqtt_params,
                    args.webrtc_options, args.timeout, ssh_extra)


if __name__ == "__main__":
    main()
