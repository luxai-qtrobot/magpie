"""Multi-peer WebRTC over a ROUTER/DEALER ZMQ signaling room."""

import socket
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip("aiortc")
pytest.importorskip("zmq")

from luxai.magpie.transport.webrtc import (  # noqa: E402
    WebRTCConnection, WebRtcSignaler, WebRtcStreamReader, WebRtcStreamWriter,
)


def test_rapidly_created_connections_have_distinct_peer_ids():
    class NoopSignaler(WebRtcSignaler):
        session_id = "room"

        def publish(self, payload):
            pass

        def subscribe(self, callback):
            pass

        def unsubscribe(self):
            pass

        def disconnect(self):
            pass

    peers = [WebRTCConnection(NoopSignaler()) for _ in range(100)]
    assert len({peer.peer_id for peer in peers}) == len(peers)


def test_zmq_multiplex_host_fans_out_to_two_clients():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    endpoint = f"tcp://127.0.0.1:{port}"
    connections = [
        WebRTCConnection.with_zmq(
            endpoint, "zmq-multi", bind=(index == 0), multiplex=True,
            role="host" if index == 0 else "client",
        ) for index in range(3)
    ]
    host, first, second = connections
    readers = []
    writer = None
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(conn.connect, 15) for conn in connections]
            assert all(future.result(timeout=20) for future in futures)
        deadline = time.monotonic() + 8
        while len(host.peer_ids) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(host.peer_ids) == 2
        assert len(first.peer_ids) == len(second.peer_ids) == 1
        readers = [
            WebRtcStreamReader(first, topic="state"),
            WebRtcStreamReader(second, topic="state"),
        ]
        writer = WebRtcStreamWriter(host, queue_size=0)
        writer.write({"count": 1}, topic="state")
        for reader in readers:
            assert reader.read(timeout=5) == ({"count": 1}, "state")
    finally:
        if writer:
            writer.close()
        for reader in readers:
            reader.close()
        for conn in connections:
            conn.disconnect()
