#!/usr/bin/env python3
"""
magpie-ssh-server-webrtc — accept SSH tunnel connections over WebRTC.

Monitors the WebRTC signal topic for new client "hello" messages.
For each new peer_id, creates a dedicated WebRTCConnection, then bridges
the WebRTC data channel ↔ local sshd.

Usage:
    magpie-ssh-server-webrtc --signaling mqtt://broker:1883 my-node
    magpie-ssh-server-webrtc --signaling mqtts://broker:8883 my-node \\
        --mqtt-params '@/etc/magpie/mqtt.json' --webrtc-options '@/etc/magpie/webrtc.json'
"""
import argparse
import sys
import threading
import time

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
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.transport.mqtt import MqttConnection
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options, get_mqtt_protocol_version
from luxai.magpie.tools._webrtc_tools_common import webrtc_options_type, build_webrtc_options
from luxai.magpie.tools.ssh._ssh_tools_common import (
    SSH_STREAM_TOPIC_UP, SSH_STREAM_TOPIC_DOWN,
    bridge_socket_to_writer, bridge_reader_to_socket, connect_sshd,
)


class _WebRtcSshSession:
    """One SSH tunnel session: WebRTC data channel ↔ local sshd socket."""

    def __init__(self, peer_id: str, conn: WebRTCConnection,
                 sshd_host: str, sshd_port: int):
        self._peer_id   = peer_id
        self._conn      = conn
        self._sshd_host = sshd_host
        self._sshd_port = sshd_port
        self._stop      = threading.Event()
        self._sock      = None

    def start(self) -> bool:
        try:
            self._sock = connect_sshd(self._sshd_host, self._sshd_port)
        except OSError as e:
            Logger.error(f"[ssh-server-webrtc] {self._peer_id}: cannot reach sshd: {e}")
            return False

        # queue_size=0 → direct read/write on the WebRTC data channel
        writer = WebRtcStreamWriter(connection=self._conn)
        reader = WebRtcStreamReader(connection=self._conn, topic=SSH_STREAM_TOPIC_UP)

        t1 = threading.Thread(
            target=bridge_reader_to_socket,
            args=(reader, self._sock, self._stop),
            daemon=True,
            name=f"ssh-webrtc-r2s-{self._peer_id[:8]}",
        )
        t2 = threading.Thread(
            target=bridge_socket_to_writer,
            args=(self._sock, writer, SSH_STREAM_TOPIC_DOWN, self._stop),
            daemon=True,
            name=f"ssh-webrtc-s2w-{self._peer_id[:8]}",
        )
        t1.start()
        t2.start()

        threading.Thread(
            target=self._wait_and_cleanup,
            args=(reader, writer),
            daemon=True,
            name=f"ssh-webrtc-cleanup-{self._peer_id[:8]}",
        ).start()

        Logger.info(f"[ssh-server-webrtc] session {self._peer_id}: started → sshd {self._sshd_host}:{self._sshd_port}")
        return True

    def _wait_and_cleanup(self, reader, writer) -> None:
        self._stop.wait()
        reader.close()
        writer.close()
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            self._conn.disconnect()
        except Exception:
            pass
        Logger.info(f"[ssh-server-webrtc] session {self._peer_id}: closed")

    def is_alive(self) -> bool:
        return not self._stop.is_set() and self._conn.is_connected

    def stop(self) -> None:
        self._stop.set()


class WebRtcSshServer:
    """
    Monitors the WebRTC signal topic (same pattern as ProviderBridge) and
    spawns one WebRTCConnection + sshd bridge per incoming client peer.
    """

    def __init__(self, node_id: str, signaling_url: str,
                 mqtt_params: dict, webrtc_options: dict,
                 sshd_host: str, sshd_port: int, connect_timeout: float):
        self._node_id         = node_id
        self._signaling_url   = signaling_url
        self._mqtt_params     = mqtt_params
        self._webrtc_opts     = webrtc_options
        self._sshd_host       = sshd_host
        self._sshd_port       = sshd_port
        self._connect_timeout = connect_timeout

        self._sessions: dict      = {}
        self._sessions_lock       = threading.Lock()
        self._known_peer_ids: set = set()
        self._gateway_peer_ids: set = set()
        self._serializer          = MsgpackSerializer()

        self._monitor_conn: MqttConnection = None

    def start(self, timeout: float = 10.0) -> bool:
        scheme = self._signaling_url.split("://")[0].lower() if "://" in self._signaling_url else ""
        if scheme not in ("mqtt", "mqtts", "ws", "wss"):
            Logger.error(
                f"[ssh-server-webrtc] signaling URL must be mqtt:// or mqtts://, got: {self._signaling_url}"
            )
            return False

        opts = build_mqtt_options(self._mqtt_params)
        self._monitor_conn = MqttConnection(self._signaling_url, options=opts, protocol_version=get_mqtt_protocol_version(self._mqtt_params))
        if not self._monitor_conn.connect(timeout=timeout):
            Logger.error(f"[ssh-server-webrtc] cannot connect to signaling broker at {self._signaling_url}")
            return False

        signal_topic = f"magpie/webrtc/{self._node_id}/signal"
        self._monitor_conn.add_subscription(signal_topic, self._on_signal_message)
        Logger.info(f"[ssh-server-webrtc] ready — node={self._node_id}  signaling={self._signaling_url}")
        Logger.info(f"[ssh-server-webrtc] monitoring {signal_topic}")
        return True

    def _on_signal_message(self, payload_bytes: bytes, topic: str) -> None:
        try:
            msg = self._serializer.deserialize(payload_bytes)
        except Exception:
            return
        if not isinstance(msg, dict) or msg.get("type") != "hello":
            return
        remote_peer_id = msg.get("peer_id", "")
        if not remote_peer_id:
            return

        with self._sessions_lock:
            if remote_peer_id in self._gateway_peer_ids:
                return  # our own connection's hello — ignore
            if remote_peer_id in self._sessions or remote_peer_id in self._known_peer_ids:
                return
            self._known_peer_ids.add(remote_peer_id)

        Logger.info(f"[ssh-server-webrtc] new client peer_id={remote_peer_id}")
        threading.Thread(
            target=self._spawn_session,
            args=(remote_peer_id,),
            daemon=True,
            name=f"ssh-webrtc-spawn-{remote_peer_id[:8]}",
        ).start()

    def _spawn_session(self, remote_peer_id: str) -> None:
        conn = None
        try:
            conn = WebRTCConnection.with_mqtt(
                broker_url=self._signaling_url,
                session_id=self._node_id,
                mqtt_options=build_mqtt_options(self._mqtt_params),
                options=build_webrtc_options(self._webrtc_opts, self._signaling_url),
                reconnect=False,
            )
            with self._sessions_lock:
                self._gateway_peer_ids.add(conn.peer_id)

            if not conn.connect(timeout=self._connect_timeout):
                Logger.warning(f"[ssh-server-webrtc] WebRTC handshake timed out for {remote_peer_id}")
                with self._sessions_lock:
                    self._known_peer_ids.discard(remote_peer_id)
                    self._gateway_peer_ids.discard(conn.peer_id)
                return

            session = _WebRtcSshSession(remote_peer_id, conn,
                                        self._sshd_host, self._sshd_port)
            if not session.start():
                conn.disconnect()
                with self._sessions_lock:
                    self._known_peer_ids.discard(remote_peer_id)
                    self._gateway_peer_ids.discard(conn.peer_id)
                return

            with self._sessions_lock:
                self._sessions[remote_peer_id] = session
                self._known_peer_ids.discard(remote_peer_id)

        except Exception as e:
            Logger.error(f"[ssh-server-webrtc] spawn failed for {remote_peer_id}: {e}")
            with self._sessions_lock:
                self._known_peer_ids.discard(remote_peer_id)
                if conn is not None:
                    self._gateway_peer_ids.discard(conn.peer_id)

    def reap_dead_sessions(self) -> None:
        with self._sessions_lock:
            dead = [p for p, s in self._sessions.items() if not s.is_alive()]
        for p in dead:
            Logger.debug(f"[ssh-server-webrtc] reaping dead session {p}")
            with self._sessions_lock:
                self._sessions.pop(p, None)

    def stop(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            s.stop()
        if self._monitor_conn:
            try:
                self._monitor_conn.disconnect()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-ssh-server-webrtc",
        description="Accept SSH tunnel connections over WebRTC and forward to local sshd.",
    )
    parser.add_argument("node_id", type=str,
                        help="Node identifier — clients must use the same value")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling broker URL: mqtt://host:port or mqtts://host:port "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        dest="mqtt_params", metavar="JSON|@FILE",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=webrtc_options_type, default=None,
                        dest="webrtc_options", metavar="JSON|@FILE",
                        help="WebRTC options (STUN/TURN servers, …) as JSON or @file.json.")
    parser.add_argument("--sshd-host", type=str, default="127.0.0.1",
                        help="Local sshd host to forward connections to (default: 127.0.0.1)")
    parser.add_argument("--sshd-port", type=int, default=22,
                        help="Local sshd port (default: 22)")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="WebRTC handshake timeout in seconds (default: 30)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    server = WebRtcSshServer(
        node_id=args.node_id,
        signaling_url=args.signaling,
        mqtt_params=args.mqtt_params,
        webrtc_options=args.webrtc_options,
        sshd_host=args.sshd_host,
        sshd_port=args.sshd_port,
        connect_timeout=args.timeout,
    )

    if not server.start():
        sys.exit(1)

    try:
        while True:
            time.sleep(5.0)
            server.reap_dead_sessions()
    except KeyboardInterrupt:
        Logger.info("[ssh-server-webrtc] shutting down...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
