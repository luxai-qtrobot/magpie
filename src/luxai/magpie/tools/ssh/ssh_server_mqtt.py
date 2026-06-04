#!/usr/bin/env python3
"""
magpie-ssh-server-mqtt — accept SSH tunnel connections over MQTT.

Subscribes to  magpie/ssh/<node_id>/+/up  and, for every new session ULID
that appears, opens a TCP connection to the local sshd and bridges bytes
bidirectionally.

Usage:
    magpie-ssh-server-mqtt mqtt://broker:1883 my-node
    magpie-ssh-server-mqtt mqtts://broker:8883 my-node --mqtt-params '@/etc/magpie/mqtt.json'
    magpie-ssh-server-mqtt mqtt://broker:1883 my-node --sshd-port 22 -v
"""
import argparse
import sys
import threading
import time

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
from luxai.magpie.transport import MqttConnection, MqttStreamWriter, MqttStreamReader
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options
from luxai.magpie.tools.ssh._ssh_tools_common import (
    mqtt_up_topic, mqtt_down_topic, mqtt_wildcard_up, extract_session_ulid,
    bridge_socket_to_writer, bridge_reader_to_socket, connect_sshd,
)


class _SshSession:
    """One SSH tunnel session: MQTT stream ↔ local sshd socket."""

    def __init__(self, session_ulid: str, conn: MqttConnection,
                 node_id: str, sshd_host: str, sshd_port: int):
        self._ulid    = session_ulid
        self._conn    = conn
        self._node_id = node_id
        self._sshd_host = sshd_host
        self._sshd_port = sshd_port
        self._stop    = threading.Event()
        self._threads: list = []
        self._sock    = None

    def start(self, first_chunk: bytes) -> bool:
        try:
            self._sock = connect_sshd(self._sshd_host, self._sshd_port)
        except OSError as e:
            Logger.error(f"[ssh-server-mqtt] session {self._ulid}: cannot reach sshd: {e}")
            return False

        up   = mqtt_up_topic(self._node_id, self._ulid)
        down = mqtt_down_topic(self._node_id, self._ulid)

        # queue_size=0 → direct read/write, no drop-oldest queue
        reader = MqttStreamReader(self._conn, topic=up, queue_size=0)
        writer = MqttStreamWriter(self._conn, queue_size=0)

        # Deliver the first SSH chunk that triggered session creation
        if first_chunk:
            try:
                self._sock.sendall(first_chunk)
            except OSError as e:
                Logger.error(f"[ssh-server-mqtt] session {self._ulid}: sshd write failed: {e}")
                reader.close()
                self._sock.close()
                return False

        t1 = threading.Thread(
            target=bridge_reader_to_socket,
            args=(reader, self._sock, self._stop),
            daemon=True,
            name=f"ssh-mqtt-r2s-{self._ulid[:8]}",
        )
        t2 = threading.Thread(
            target=bridge_socket_to_writer,
            args=(self._sock, writer, down, self._stop),
            daemon=True,
            name=f"ssh-mqtt-s2w-{self._ulid[:8]}",
        )
        t1.start()
        t2.start()
        self._threads = [t1, t2]

        # Cleanup thread
        threading.Thread(
            target=self._wait_and_cleanup,
            args=(reader, writer),
            daemon=True,
            name=f"ssh-mqtt-cleanup-{self._ulid[:8]}",
        ).start()

        Logger.info(f"[ssh-server-mqtt] session {self._ulid}: started → sshd {self._sshd_host}:{self._sshd_port}")
        return True

    def _wait_and_cleanup(self, reader, writer) -> None:
        self._stop.wait()
        reader.close()
        writer.close()
        try:
            self._sock.close()
        except OSError:
            pass
        Logger.info(f"[ssh-server-mqtt] session {self._ulid}: closed")

    def is_alive(self) -> bool:
        return not self._stop.is_set()

    def stop(self) -> None:
        self._stop.set()


class MqttSshServer:
    """Listens for new MQTT SSH sessions and manages their lifecycle."""

    def __init__(self, uri: str, node_id: str,
                 mqtt_params: dict, sshd_host: str, sshd_port: int):
        self._uri        = uri
        self._node_id    = node_id
        self._sshd_host  = sshd_host
        self._sshd_port  = sshd_port
        self._sessions: dict = {}
        self._sessions_lock  = threading.Lock()
        self._known_ulids: set = set()
        self._conn: MqttConnection = None

        opts = build_mqtt_options(mqtt_params)
        self._conn = MqttConnection(uri, options=opts)

    def start(self, timeout: float = 10.0) -> bool:
        if not self._conn.connect(timeout=timeout):
            Logger.error(f"[ssh-server-mqtt] cannot connect to broker at {self._uri}")
            return False

        wildcard = mqtt_wildcard_up(self._node_id)
        self._conn.add_subscription(wildcard, self._on_message)
        Logger.info(f"[ssh-server-mqtt] ready — node={self._node_id}  broker={self._uri}")
        Logger.info(f"[ssh-server-mqtt] listening on {wildcard}")
        return True

    def _on_message(self, payload_bytes: bytes, topic: str) -> None:
        from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
        ulid = extract_session_ulid(topic, self._node_id)
        if not ulid:
            return

        with self._sessions_lock:
            if ulid in self._sessions or ulid in self._known_ulids:
                # deliver to existing session via its own reader subscription
                return
            self._known_ulids.add(ulid)

        # Deserialize the first chunk
        try:
            data = MsgpackSerializer().deserialize(payload_bytes)
        except Exception:
            data = payload_bytes

        Logger.info(f"[ssh-server-mqtt] new session {ulid}")
        threading.Thread(
            target=self._spawn_session,
            args=(ulid, data),
            daemon=True,
            name=f"ssh-mqtt-spawn-{ulid[:8]}",
        ).start()

    def _spawn_session(self, ulid: str, first_chunk: bytes) -> None:
        session = _SshSession(ulid, self._conn, self._node_id,
                              self._sshd_host, self._sshd_port)
        if not session.start(first_chunk):
            with self._sessions_lock:
                self._known_ulids.discard(ulid)
            return

        with self._sessions_lock:
            self._sessions[ulid] = session
            self._known_ulids.discard(ulid)

    def reap_dead_sessions(self) -> None:
        with self._sessions_lock:
            dead = [u for u, s in self._sessions.items() if not s.is_alive()]
        for u in dead:
            Logger.debug(f"[ssh-server-mqtt] reaping dead session {u}")
            with self._sessions_lock:
                self._sessions.pop(u, None)

    def stop(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            s.stop()
        if self._conn:
            self._conn.disconnect()


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-ssh-server-mqtt",
        description="Accept SSH tunnel connections over MQTT and forward to local sshd.",
    )
    parser.add_argument("uri", type=str,
                        help="MQTT broker URI (e.g. mqtt://broker:1883 or mqtts://broker:8883)")
    parser.add_argument("node_id", type=str,
                        help="Node identifier — clients must use the same value")
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        dest="mqtt_params", metavar="JSON|@FILE",
                        help="MQTT connection options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--sshd-host", type=str, default="127.0.0.1",
                        help="Local sshd host to forward connections to (default: 127.0.0.1)")
    parser.add_argument("--sshd-port", type=int, default=22,
                        help="Local sshd port (default: 22)")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Broker connection timeout in seconds (default: 10)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    server = MqttSshServer(
        uri=args.uri,
        node_id=args.node_id,
        mqtt_params=args.mqtt_params,
        sshd_host=args.sshd_host,
        sshd_port=args.sshd_port,
    )

    if not server.start(timeout=args.timeout):
        sys.exit(1)

    try:
        while True:
            time.sleep(5.0)
            server.reap_dead_sessions()
    except KeyboardInterrupt:
        Logger.info("[ssh-server-mqtt] shutting down...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
