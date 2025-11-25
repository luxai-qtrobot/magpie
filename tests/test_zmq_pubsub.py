# tests/test_zmq_pubsub.py

import unittest
import time
import socket
from luxai.magpie.transport import ZMQPublisher
from luxai.magpie.transport import ZMQSubscriber


def _free_tcp_port():
    """Get a free localhost TCP port for testing."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    addr, port = s.getsockname()
    s.close()
    return port


def _collect_messages(sub: ZMQSubscriber, expected_count: int, timeout: float = 2.0):
    """Read up to expected_count messages from subscriber within timeout."""
    messages = []
    t0 = time.perf_counter()
    while len(messages) < expected_count and (time.perf_counter() - t0) < timeout:
        item = sub.read()
        if item is None:
            continue
        data, topic = item
        messages.append((data, topic))
    return messages


class TestZmqPubSubTCP(unittest.TestCase):
    """
    Tests over tcp://127.0.0.1 for:
      - A: basic PUB→SUB
      - B1: multiple messages (reliable)
      - C: queue_size=0 vs queue_size=1 (structural)
      - D: serialization (msgpack)
      - E: clean shutdown
    """

    def test_basic_pub_sub_tcp(self):
        port = _free_tcp_port()
        endpoint = f"tcp://127.0.0.1:{port}"

        # PUBLISHER: bind=True → bind()
        pub = ZMQPublisher(
            endpoint=endpoint,
            queue_size=10,
            bind=True,
            delivery="reliable",
        )

        time.sleep(0.5)  # let ZMQ handshake

        # SUBSCRIBER first: bind False → connect()
        sub = ZMQSubscriber(
            endpoint=endpoint,
            topic="test",
            queue_size=10,
            bind=False,
            delivery="reliable",
        )

        # small delay to let SUB connect prepare
        time.sleep(0.5)


        msgs = [
            {"i": 0},
            {"i": 1},
            {"i": 2},
        ]

        for m in msgs:
            pub.write(m, topic="test")
        
        received = _collect_messages(sub, expected_count=len(msgs), timeout=2.0)

        # Check we got all messages in order on topic "test"
        self.assertEqual(len(received), len(msgs))
        data_only = [m[0] for m in received]
        topics = [m[1] for m in received]

        self.assertEqual(data_only, msgs)
        self.assertTrue(all(t == "test" for t in topics))

        # Clean shutdown (E)
        pub.close()
        sub.close()

    def test_publisher_queue_size_zero_and_one_tcp(self):
        port = _free_tcp_port()
        endpoint = f"tcp://127.0.0.1:{port}"

        # queue_size = 0 → no background writer queue/thread
        pub0 = ZMQPublisher(
            endpoint=endpoint,
            queue_size=0,
            bind=True,
            delivery="reliable",
        )
        # StreamWriter only creates writer_queue/thread if queue_size > 0
        self.assertFalse(hasattr(pub0, "writer_queue"))
        self.assertFalse(hasattr(pub0, "thread"))
        pub0.close()

        # queue_size = 1 → background writer queue/thread exist
        port2 = _free_tcp_port()
        endpoint2 = f"tcp://127.0.0.1:{port2}"

        pub1 = ZMQPublisher(
            endpoint=endpoint2,
            queue_size=1,
            bind=True,
            delivery="reliable",
        )
        self.assertTrue(hasattr(pub1, "writer_queue"))
        self.assertTrue(hasattr(pub1, "thread"))
        self.assertTrue(pub1.thread.is_alive())
        pub1.close()

    def test_clean_shutdown_tcp(self):
        port = _free_tcp_port()
        endpoint = f"tcp://127.0.0.1:{port}"

        sub = ZMQSubscriber(
            endpoint=endpoint,
            topic="",
            queue_size=10,
            bind=True,
            delivery="reliable",
        )
        time.sleep(0.5)  # let ZMQ handshake

        pub = ZMQPublisher(
            endpoint=endpoint,
            queue_size=10,
            bind=False,
            delivery="reliable",
        )
        time.sleep(0.5)

        pub.write({"msg": "hello"}, topic="t")
        _ = _collect_messages(sub, expected_count=1, timeout=1.0)

        pub.close()
        sub.close()

        # ZMQ sockets should be closed without raising
        self.assertTrue(pub.socket.closed)
        self.assertTrue(sub.socket.closed)


class TestZmqPubSubInproc(unittest.TestCase):
    """
    Same tests as above, but using inproc:// transport.
    """

    def test_basic_pub_sub_inproc(self):
        endpoint = "inproc://test-basic"

        # For inproc: best practice is pub.bind first, then sub.connect.
        pub = ZMQPublisher(
            endpoint=endpoint,
            queue_size=10,
            bind=True,
            delivery="reliable",
        )

        sub = ZMQSubscriber(
            endpoint=endpoint,
            topic="test",
            queue_size=10,
            bind=False,
            delivery="reliable",
        )

        time.sleep(0.5)  # allow subscription/connection

        msgs = [
            {"i": 10},
            {"i": 11},
            {"i": 12},
        ]

        for m in msgs:
            pub.write(m, topic="test")

        received = _collect_messages(sub, expected_count=len(msgs), timeout=2.0)
        self.assertEqual(len(received), len(msgs))

        data_only = [m[0] for m in received]
        topics = [m[1] for m in received]

        self.assertEqual(data_only, msgs)
        self.assertTrue(all(t == "test" for t in topics))

        pub.close()
        sub.close()

    def test_publisher_queue_size_zero_and_one_inproc(self):
        # queue_size=0
        endpoint0 = "inproc://qs0"
        pub0 = ZMQPublisher(
            endpoint=endpoint0,
            queue_size=0,
            bind=True,
            delivery="reliable",
        )
        self.assertFalse(hasattr(pub0, "writer_queue"))
        self.assertFalse(hasattr(pub0, "thread"))
        pub0.close()

        # queue_size=1
        endpoint1 = "inproc://qs1"
        pub1 = ZMQPublisher(
            endpoint=endpoint1,
            queue_size=1,
            bind=True,
            delivery="reliable",
        )
        self.assertTrue(hasattr(pub1, "writer_queue"))
        self.assertTrue(hasattr(pub1, "thread"))
        self.assertTrue(pub1.thread.is_alive())
        pub1.close()

    def test_clean_shutdown_inproc(self):
        endpoint = "inproc://clean"

        pub = ZMQPublisher(
            endpoint=endpoint,
            queue_size=10,
            bind=True,
            delivery="reliable",
        )

        sub = ZMQSubscriber(
            endpoint=endpoint,
            topic="",
            queue_size=10,
            bind=False,
            delivery="reliable",
        )

        time.sleep(0.05)

        pub.write({"msg": "world"}, topic="x")
        _ = _collect_messages(sub, expected_count=1, timeout=1.0)

        pub.close()
        sub.close()

        self.assertTrue(pub.socket.closed)
        self.assertTrue(sub.socket.closed)


if __name__ == "__main__":    
    unittest.main()
