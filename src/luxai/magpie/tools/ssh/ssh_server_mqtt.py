#!/usr/bin/env python3
"""
magpie-ssh-server-mqtt — accept SSH tunnel connections over MQTT.

Listens on  magpie/ssh/<node_id>/rpc/req  for session requests using Magpie's
RPC protocol. For each session, subscribes to the specific up topic and bridges
magpie/ssh/<node_id>/<ulid>/up  ↔  local sshd.

Usage:
    magpie-ssh-server-mqtt mqtt://broker:1883 my-node
    magpie-ssh-server-mqtt mqtts://broker:8883 my-node --mqtt-params '@/etc/magpie/mqtt.json'
    magpie-ssh-server-mqtt mqtt://broker:1883 my-node --sshd-port 22 -v
"""
import argparse
import sys
import threading
import time
from queue import Queue, Empty

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
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.transport import MqttConnection, MqttStreamWriter, MqttRpcResponder
from luxai.magpie.nodes import ServerNode
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type, build_mqtt_options, get_mqtt_protocol_version
from luxai.magpie.tools.ssh._ssh_tools_common import (
    mqtt_up_topic, mqtt_down_topic,
    bridge_socket_to_writer, connect_sshd,
)

_serializer = MsgpackSerializer()


def _bridge_queue_to_socket(q: Queue, sock, stop_event: threading.Event) -> None:
    """Drain per-session queue into the sshd socket."""
    try:
        while not stop_event.is_set():
            try:
                data = q.get(timeout=1.0)
            except Empty:
                continue
            if not data:
                continue
            try:
                sock.sendall(data)
            except OSError:
                break
    finally:
        stop_event.set()


class _SshSession:
    """One SSH tunnel session: per-session Queue ↔ local sshd socket."""

    def __init__(self, session_ulid: str, data_queue: Queue,
                 conn: MqttConnection, node_id: str,
                 sshd_host: str, sshd_port: int):
        self._ulid      = session_ulid
        self._queue     = data_queue
        self._conn      = conn
        self._node_id   = node_id
        self._sshd_host = sshd_host
        self._sshd_port = sshd_port
        self._stop      = threading.Event()
        self._sock      = None

    def start(self) -> bool:
        try:
            self._sock = connect_sshd(self._sshd_host, self._sshd_port)
        except OSError as e:
            Logger.error(f"[ssh-server-mqtt] session {self._ulid}: cannot reach sshd: {e}")
            return False

        down   = mqtt_down_topic(self._node_id, self._ulid)
        writer = MqttStreamWriter(self._conn, queue_size=0)

        t1 = threading.Thread(
            target=_bridge_queue_to_socket,
            args=(self._queue, self._sock, self._stop),
            daemon=True,
            name=f"ssh-mqtt-q2s-{self._ulid[:8]}",
        )
        t2 = threading.Thread(
            target=bridge_socket_to_writer,
            args=(self._sock, writer, down, self._stop),
            daemon=True,
            name=f"ssh-mqtt-s2w-{self._ulid[:8]}",
        )
        t1.start()
        t2.start()

        threading.Thread(
            target=self._cleanup,
            args=(writer,),
            daemon=True,
            name=f"ssh-mqtt-cleanup-{self._ulid[:8]}",
        ).start()

        Logger.info(f"[ssh-server-mqtt] session {self._ulid}: started → sshd {self._sshd_host}:{self._sshd_port}")
        return True

    def _cleanup(self, writer) -> None:
        self._stop.wait()
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
    """
    Accepts SSH tunnel sessions via Magpie RPC on magpie/ssh/<node_id>/rpc/req.

    Each session request carries a unique ULID. The handler subscribes to the
    specific up topic, connects to sshd, and returns "ready" — all before the
    RPC response is sent, so the client can trust the tunnel is fully set up.
    """

    def __init__(self, uri: str, node_id: str,
                 mqtt_params: dict, sshd_host: str, sshd_port: int):
        self._uri       = uri
        self._node_id   = node_id
        self._sshd_host = sshd_host
        self._sshd_port = sshd_port

        self._sessions: dict          = {}
        self._session_queues: dict    = {}
        self._session_callbacks: dict = {}
        self._lock                    = threading.Lock()

        opts = build_mqtt_options(mqtt_params)
        self._conn = MqttConnection(uri, options=opts, protocol_version=get_mqtt_protocol_version(mqtt_params))

    def start(self, timeout: float = 10.0) -> "MqttRpcResponder | None":
        if not self._conn.connect(timeout=timeout):
            Logger.error(f"[ssh-server-mqtt] cannot connect to broker at {self._uri}")
            return None

        service = f"magpie/ssh/{self._node_id}"
        responder = MqttRpcResponder(self._conn, service_name=service)
        Logger.info(f"[ssh-server-mqtt] ready — node={self._node_id}  broker={self._uri}")
        Logger.info(f"[ssh-server-mqtt] listening on {service}/rpc/req")
        return responder

    def on_session_request(self, request: dict) -> dict:
        """RPC handler — runs in a ServerNode worker thread per session."""
        ulid = request.get("session_id") if isinstance(request, dict) else None
        if not ulid:
            Logger.warning("[ssh-server-mqtt] invalid session request: missing session_id")
            return {"status": "error", "reason": "missing session_id"}

        Logger.info(f"[ssh-server-mqtt] new session {ulid}")

        q  = Queue()
        up = mqtt_up_topic(self._node_id, ulid)

        def _on_up(payload_bytes: bytes, t: str, _ulid=ulid) -> None:
            try:
                data = _serializer.deserialize(payload_bytes)
            except Exception:
                data = payload_bytes
            with self._lock:
                if _ulid in self._session_queues:
                    self._session_queues[_ulid].put(data)

        with self._lock:
            self._session_queues[ulid]    = q
            self._session_callbacks[ulid] = _on_up
        self._conn.add_subscription(up, _on_up)

        session = _SshSession(ulid, q, self._conn, self._node_id,
                              self._sshd_host, self._sshd_port)
        if not session.start():
            with self._lock:
                self._session_queues.pop(ulid, None)
                self._session_callbacks.pop(ulid, None)
            self._conn.remove_subscription(up, _on_up)
            return {"status": "error",
                    "reason": f"cannot reach sshd at {self._sshd_host}:{self._sshd_port}"}

        with self._lock:
            self._sessions[ulid] = session

        return {"status": "ready"}

    def reap_dead_sessions(self) -> None:
        with self._lock:
            dead = [u for u, s in self._sessions.items() if not s.is_alive()]
        for u in dead:
            Logger.debug(f"[ssh-server-mqtt] reaping dead session {u}")
            with self._lock:
                self._sessions.pop(u, None)
                self._session_queues.pop(u, None)
                cb = self._session_callbacks.pop(u, None)
            if cb:
                self._conn.remove_subscription(mqtt_up_topic(self._node_id, u), cb)

    def stop(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
        for s in sessions:
            s.stop()
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

    responder = server.start(timeout=args.timeout)
    if responder is None:
        sys.exit(1)

    server_node = ServerNode(
        responder=responder,
        handler=server.on_session_request,
        name="ssh-server-mqtt",
    )

    try:
        while True:
            time.sleep(5.0)
            server.reap_dead_sessions()
    except KeyboardInterrupt:
        Logger.info("[ssh-server-mqtt] shutting down...")
    finally:
        server_node.terminate()
        server.stop()


if __name__ == "__main__":
    main()
