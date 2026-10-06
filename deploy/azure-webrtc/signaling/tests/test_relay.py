import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from http_signaling import InMemoryRelay, SignalingHTTP


class InMemoryRelayTests(unittest.TestCase):
    def test_peer_and_session_limits(self):
        relay = InMemoryRelay(max_sessions=1, max_peers_per_session=2)

        self.assertTrue(relay.join("session-one", "peer-one"))
        self.assertTrue(relay.join("session-one", "peer-two"))
        self.assertFalse(relay.join("session-one", "peer-three"))
        self.assertFalse(relay.join("session-two", "peer-one"))

    def test_message_delivery_and_deduplication(self):
        relay = InMemoryRelay()
        relay.join("session-one", "peer-one")
        relay.join("session-one", "peer-two")

        self.assertTrue(relay.send("session-one", "peer-one", "message-1", b"hello"))
        self.assertEqual(relay.receive("session-one", "peer-two", 0, 0), (1, b"hello"))

        self.assertTrue(relay.send("session-one", "peer-one", "message-1", b"duplicate"))
        self.assertEqual(relay.receive("session-one", "peer-two", 1, 0), (0, b""))

    def test_queue_is_bounded(self):
        relay = InMemoryRelay(max_queued_messages=2)
        relay.join("session-one", "peer-one")
        relay.join("session-one", "peer-two")
        relay.send("session-one", "peer-one", "message-1", b"one")
        relay.send("session-one", "peer-one", "message-2", b"two")
        relay.send("session-one", "peer-one", "message-3", b"three")

        sequence, payload = relay.receive("session-one", "peer-two", 0, 0)
        self.assertEqual((sequence, payload), (2, b"two"))


class SignalingHTTPTests(unittest.TestCase):
    def test_complete_join_send_receive_flow(self):
        protocol = SignalingHTTP(InMemoryRelay())
        first = "/sessions/test-session/peers/peer-one"
        second = "/sessions/test-session/peers/peer-two"

        self.assertEqual(protocol.handle("PUT", first, body=b"host").status, 204)
        self.assertEqual(protocol.handle("PUT", second, body=b"client").status, 204)

        replay = protocol.handle("GET", second + "/messages", query="after=0&wait=0")
        self.assertEqual((replay.status, replay.body), (200, b"host"))

        sent = protocol.handle(
            "POST",
            first + "/messages",
            headers={"X-Magpie-Message-Id": "message-1"},
            body=b"offer",
        )
        self.assertEqual(sent.status, 204)

        received = protocol.handle(
            "GET",
            second + "/messages",
            query="after=1&wait=0",
        )
        self.assertEqual((received.status, received.body), (200, b"offer"))

    def test_rejects_unsafe_identifiers_and_large_payloads(self):
        protocol = SignalingHTTP(InMemoryRelay(), max_message_bytes=4)

        self.assertEqual(
            protocol.handle("PUT", "/sessions/../peers/peer-one").status,
            404,
        )
        self.assertEqual(
            protocol.handle(
                "PUT",
                "/sessions/test-session/peers/peer-one",
                body=b"large",
            ).status,
            413,
        )


if __name__ == "__main__":
    unittest.main()
