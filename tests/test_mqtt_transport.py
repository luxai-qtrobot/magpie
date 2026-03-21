"""
Unit tests for the MQTT transport layer.

All tests in TestMqttPublisher, TestMqttSubscriber, TestMqttRpcRequester, and
TestMqttRpcResponder run entirely in-process using FakeMqttConnection — no
network or real broker is required.

Integration tests (TestMqttIntegration) connect to the free HiveMQ public
broker and are skipped by default.  Enable them by setting the environment
variable MAGPIE_INTEGRATION_TESTS=1 before running pytest/unittest:

    MAGPIE_INTEGRATION_TESTS=1 python -m pytest tests/test_mqtt_transport.py
"""

import os
import threading
import time
import unittest

import paho.mqtt.client as _paho

from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.transport.mqtt.mqtt_connection import _parse_mqtt_uri
from luxai.magpie.transport.mqtt.mqtt_options import MqttOptions
from luxai.magpie.transport.mqtt.mqtt_publisher import MqttPublisher
from luxai.magpie.transport.mqtt.mqtt_rpc_requester import MqttRpcRequester
from luxai.magpie.transport.mqtt.mqtt_rpc_responder import MqttRpcResponder
from luxai.magpie.transport.mqtt.mqtt_subscriber import MqttSubscriber

_SERIALIZER = MsgpackSerializer()

# ---------------------------------------------------------------------------
# Fake broker connection — no network needed
# ---------------------------------------------------------------------------

class FakeMqttConnection:
    """
    In-process stand-in for MqttConnection.

    Supports add_subscription / remove_subscription / publish exactly as the
    real class does.  Call inject_message() to simulate the broker delivering
    a message to all matching subscribers.
    """

    def __init__(self):
        self.client_id = "fake-client"
        self.uri = "mqtt://fake"
        self.options = MqttOptions()
        self._subscriptions: dict = {}
        self._sub_lock = threading.Lock()
        self.published: list = []   # [(topic, payload, qos, retain), ...]
        self._connected = True

    def add_subscription(self, topic, callback, qos=None):
        with self._sub_lock:
            if topic not in self._subscriptions:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(callback)

    def remove_subscription(self, topic, callback):
        with self._sub_lock:
            if topic not in self._subscriptions:
                return
            try:
                self._subscriptions[topic].remove(callback)
            except ValueError:
                pass
            if not self._subscriptions[topic]:
                del self._subscriptions[topic]

    def publish(self, topic, payload, qos=None, retain=None):
        self.published.append((topic, payload, qos, retain))

    def inject_message(self, topic: str, payload: bytes):
        """Simulate the broker delivering a message to all matching subscribers."""
        with self._sub_lock:
            matching = []
            for pattern, callbacks in self._subscriptions.items():
                if _paho.topic_matches_sub(pattern, topic):
                    matching.extend(list(callbacks))
        for cb in matching:
            cb(payload, topic)

    @property
    def is_connected(self):
        return self._connected


# ---------------------------------------------------------------------------
# URI parsing tests (pure logic, no Paho needed)
# ---------------------------------------------------------------------------

class TestMqttUriParsing(unittest.TestCase):

    def _parse(self, uri):
        return _parse_mqtt_uri(uri)

    def test_mqtt_default_port(self):
        host, port, path, tls, ws = self._parse("mqtt://broker.example.com")
        self.assertEqual(host, "broker.example.com")
        self.assertEqual(port, 1883)
        self.assertFalse(tls)
        self.assertFalse(ws)

    def test_mqtt_explicit_port(self):
        _, port, _, tls, ws = self._parse("mqtt://broker.example.com:1884")
        self.assertEqual(port, 1884)
        self.assertFalse(tls)
        self.assertFalse(ws)

    def test_mqtts_default_port(self):
        _, port, _, tls, ws = self._parse("mqtts://broker.example.com")
        self.assertEqual(port, 8883)
        self.assertTrue(tls)
        self.assertFalse(ws)

    def test_ws_default_port(self):
        _, port, path, tls, ws = self._parse("ws://broker.example.com/mqtt")
        self.assertEqual(port, 9001)
        self.assertFalse(tls)
        self.assertTrue(ws)
        self.assertEqual(path, "/mqtt")

    def test_wss_default_port(self):
        _, port, _, tls, ws = self._parse("wss://broker.example.com:8884/mqtt")
        self.assertEqual(port, 8884)
        self.assertTrue(tls)
        self.assertTrue(ws)

    def test_invalid_scheme_raises(self):
        with self.assertRaises(ValueError):
            self._parse("tcp://broker.example.com:1883")

    def test_missing_host_raises(self):
        with self.assertRaises(ValueError):
            self._parse("mqtt:///no-host")


# ---------------------------------------------------------------------------
# MqttPublisher tests
# ---------------------------------------------------------------------------

class TestMqttPublisher(unittest.TestCase):

    def setUp(self):
        self.conn = FakeMqttConnection()
        self.pub = MqttPublisher(self.conn, queue_size=5)

    def tearDown(self):
        self.pub.close()

    def test_write_serializes_and_publishes(self):
        self.pub.write({"key": "value"}, topic="sensor/temp")
        # Give the writer thread a moment to flush
        time.sleep(0.1)
        self.assertEqual(len(self.conn.published), 1)
        topic, payload, _, _ = self.conn.published[0]
        self.assertEqual(topic, "sensor/temp")
        self.assertEqual(_SERIALIZER.deserialize(payload), {"key": "value"})

    def test_write_multiple_messages(self):
        for i in range(3):
            self.pub.write({"i": i}, topic="sensor/temp")
        time.sleep(0.2)
        self.assertEqual(len(self.conn.published), 3)
        values = [_SERIALIZER.deserialize(p[1])["i"] for p in self.conn.published]
        self.assertEqual(values, [0, 1, 2])

    def test_write_without_topic_drops_message(self):
        self.pub.write({"x": 1}, topic=None)
        time.sleep(0.1)
        self.assertEqual(len(self.conn.published), 0)

    def test_qos_override_forwarded_to_publish(self):
        conn = FakeMqttConnection()
        pub = MqttPublisher(conn, qos=2)
        pub.write({"a": 1}, topic="t/a")
        time.sleep(0.1)
        _, _, qos, _ = conn.published[0]
        self.assertEqual(qos, 2)
        pub.close()

    def test_close_is_idempotent(self):
        self.pub.close()
        self.pub.close()   # second call must not raise


# ---------------------------------------------------------------------------
# MqttSubscriber tests
# ---------------------------------------------------------------------------

class TestMqttSubscriber(unittest.TestCase):

    def setUp(self):
        self.conn = FakeMqttConnection()
        self.sub = MqttSubscriber(self.conn, topic="sensor/temp")

    def tearDown(self):
        self.sub.close()

    def test_registers_subscription(self):
        self.assertIn("sensor/temp", self.conn._subscriptions)

    def test_receive_message(self):
        payload = _SERIALIZER.serialize({"v": 42})
        self.conn.inject_message("sensor/temp", payload)
        data, topic = self.sub.read(timeout=2.0)
        self.assertEqual(data, {"v": 42})
        self.assertEqual(topic, "sensor/temp")

    def test_receive_multiple_messages(self):
        for i in range(3):
            self.conn.inject_message("sensor/temp", _SERIALIZER.serialize({"i": i}))
        received = []
        for _ in range(3):
            data, _ = self.sub.read(timeout=2.0)
            received.append(data["i"])
        self.assertEqual(received, [0, 1, 2])

    def test_timeout_raises(self):
        with self.assertRaises(TimeoutError):
            self.sub.read(timeout=0.2)

    def test_wildcard_subscription(self):
        conn = FakeMqttConnection()
        sub = MqttSubscriber(conn, topic="sensor/+")
        conn.inject_message("sensor/temp", _SERIALIZER.serialize({"t": 1}))
        conn.inject_message("sensor/humidity", _SERIALIZER.serialize({"h": 2}))
        r1, t1 = sub.read(timeout=2.0)
        r2, t2 = sub.read(timeout=2.0)
        self.assertEqual(r1, {"t": 1})
        self.assertEqual(r2, {"h": 2})
        sub.close()

    def test_close_removes_subscription(self):
        self.sub.close()
        time.sleep(0.1)
        self.assertNotIn("sensor/temp", self.conn._subscriptions)

    def test_close_is_idempotent(self):
        self.sub.close()
        self.sub.close()

    def test_multiple_subscribers_same_topic(self):
        conn = FakeMqttConnection()
        sub1 = MqttSubscriber(conn, topic="t/shared")
        sub2 = MqttSubscriber(conn, topic="t/shared")
        payload = _SERIALIZER.serialize({"x": 99})
        conn.inject_message("t/shared", payload)
        d1, _ = sub1.read(timeout=2.0)
        d2, _ = sub2.read(timeout=2.0)
        self.assertEqual(d1, {"x": 99})
        self.assertEqual(d2, {"x": 99})
        sub1.close()
        sub2.close()


# ---------------------------------------------------------------------------
# MqttRpcResponder tests
# ---------------------------------------------------------------------------

class TestMqttRpcResponder(unittest.TestCase):

    def setUp(self):
        self.conn = FakeMqttConnection()
        self.responder = MqttRpcResponder(self.conn, service_name="myrobot/motion")
        self._req_topic = "myrobot/motion/rpc/req"

    def tearDown(self):
        self.responder.close()

    def _make_request(self, rid, payload, reply_to="magpie/rpc/client/rep"):
        return _SERIALIZER.serialize({
            "rid": rid,
            "reply_to": reply_to,
            "payload": payload,
        })

    def test_registers_subscription(self):
        self.assertIn(self._req_topic, self.conn._subscriptions)

    def test_recv_returns_payload_and_sends_ack(self):
        self.conn.inject_message(
            self._req_topic,
            self._make_request("rid-1", {"x": 10}, reply_to="client/rep"),
        )
        payload, ctx = self.responder._transport_recv(timeout=1.0)
        self.assertEqual(payload, {"x": 10})
        self.assertEqual(ctx["rid"], "rid-1")
        self.assertEqual(ctx["reply_to"], "client/rep")

        # ACK must have been published
        self.assertEqual(len(self.conn.published), 1)
        ack_topic, ack_bytes, _, _ = self.conn.published[0]
        self.assertEqual(ack_topic, "client/rep")
        ack = _SERIALIZER.deserialize(ack_bytes)
        self.assertTrue(ack["ack"])
        self.assertEqual(ack["rid"], "rid-1")

    def test_recv_timeout(self):
        with self.assertRaises(TimeoutError):
            self.responder._transport_recv(timeout=0.2)

    def test_malformed_request_raises(self):
        bad = _SERIALIZER.serialize({"no_rid": True})
        self.conn.inject_message(self._req_topic, bad)
        time.sleep(0.05)   # let the callback enqueue the message
        with self.assertRaises(RuntimeError):
            self.responder._transport_recv(timeout=0.5)

    def test_handle_once_full_round_trip(self):
        self.conn.inject_message(
            self._req_topic,
            self._make_request("rid-2", {"n": 5}, reply_to="client/rep"),
        )

        def handler(req):
            return {"double": req["n"] * 2}

        handled = self.responder.handle_once(handler=handler, timeout=1.0)
        self.assertTrue(handled)

        # Two publishes: ACK + response
        self.assertEqual(len(self.conn.published), 2)
        resp_topic, resp_bytes, _, _ = self.conn.published[1]
        self.assertEqual(resp_topic, "client/rep")
        resp = _SERIALIZER.deserialize(resp_bytes)
        self.assertEqual(resp["payload"], {"double": 10})
        self.assertEqual(resp["rid"], "rid-2")

    def test_handle_once_timeout_returns_false(self):
        result = self.responder.handle_once(handler=lambda r: r, timeout=0.1)
        self.assertFalse(result)

    def test_close_removes_subscription(self):
        self.responder.close()
        time.sleep(0.05)
        self.assertNotIn(self._req_topic, self.conn._subscriptions)


# ---------------------------------------------------------------------------
# MqttRpcRequester tests
# ---------------------------------------------------------------------------

class TestMqttRpcRequester(unittest.TestCase):

    def setUp(self):
        self.conn = FakeMqttConnection()
        self.requester = MqttRpcRequester(
            self.conn,
            service_name="myrobot/motion",
            ack_timeout=1.0,
        )
        self._req_topic = "myrobot/motion/rpc/req"

    def tearDown(self):
        self.requester.close()

    def _simulate_broker(self, response_payload, delay_ack=0.05, delay_reply=0.1):
        """
        Background thread that watches conn.published for a request, then injects
        an ACK followed by a final reply into the requester's private reply topic.
        """
        def _run():
            deadline = time.time() + 3.0
            while time.time() < deadline:
                if self.conn.published:
                    _, raw, _, _ = self.conn.published[0]
                    req = _SERIALIZER.deserialize(raw)
                    rid = req["rid"]
                    reply_to = req["reply_to"]

                    time.sleep(delay_ack)
                    self.conn.inject_message(
                        reply_to,
                        _SERIALIZER.serialize({"rid": rid, "ack": True}),
                    )
                    time.sleep(delay_reply)
                    self.conn.inject_message(
                        reply_to,
                        _SERIALIZER.serialize({"rid": rid, "payload": response_payload}),
                    )
                    return
                time.sleep(0.01)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return t

    def test_request_publishes_to_correct_topic(self):
        broker = self._simulate_broker({"result": 42})
        result = self.requester.call({"value": 21}, timeout=3.0)
        self.assertEqual(result, {"result": 42})

        topic, raw, _, _ = self.conn.published[0]
        self.assertEqual(topic, self._req_topic)
        req = _SERIALIZER.deserialize(raw)
        self.assertIn("rid", req)
        self.assertIn("reply_to", req)
        self.assertEqual(req["payload"], {"value": 21})
        broker.join(timeout=2.0)

    def test_basic_rpc_round_trip(self):
        broker = self._simulate_broker({"answer": 99})
        result = self.requester.call({"q": "hello"}, timeout=3.0)
        self.assertEqual(result, {"answer": 99})
        broker.join(timeout=2.0)

    def test_complex_payload(self):
        expected = {"nested": {"a": [1, 2, 3]}, "flag": True}
        broker = self._simulate_broker(expected)
        result = self.requester.call({"dummy": 1}, timeout=3.0)
        self.assertEqual(result, expected)
        broker.join(timeout=2.0)

    def test_ack_timeout_raises(self):
        # No broker simulator — ACK never arrives
        from luxai.magpie.transport.rpc_requester import AckTimeoutError
        with self.assertRaises(AckTimeoutError):
            self.requester.call({"x": 1}, timeout=0.5)

    def test_reply_timeout_after_ack(self):
        """ACK arrives but final reply never comes."""
        from luxai.magpie.transport.rpc_requester import ReplyTimeoutError

        def _send_ack_only():
            deadline = time.time() + 3.0
            while time.time() < deadline:
                if self.conn.published:
                    _, raw, _, _ = self.conn.published[0]
                    req = _SERIALIZER.deserialize(raw)
                    time.sleep(0.05)
                    self.conn.inject_message(
                        req["reply_to"],
                        _SERIALIZER.serialize({"rid": req["rid"], "ack": True}),
                    )
                    return
                time.sleep(0.01)

        t = threading.Thread(target=_send_ack_only, daemon=True)
        t.start()
        with self.assertRaises(ReplyTimeoutError):
            self.requester.call({"x": 1}, timeout=0.5)
        t.join(timeout=2.0)

    def test_multiple_sequential_calls(self):
        for i in range(3):
            self.conn.published.clear()
            broker = self._simulate_broker({"i": i})
            result = self.requester.call({"n": i}, timeout=3.0)
            self.assertEqual(result, {"i": i})
            broker.join(timeout=2.0)

    def test_close_is_idempotent(self):
        self.requester.close()
        self.requester.close()


# ---------------------------------------------------------------------------
# Integration tests — skipped unless MAGPIE_INTEGRATION_TESTS=1
# ---------------------------------------------------------------------------

_RUN_INTEGRATION = bool(int(os.environ.get("MAGPIE_INTEGRATION_TESTS", "0")))

_BROKER_URI     = "mqtt://broker.hivemq.com:1883"
_BROKER_TIMEOUT = 15.0    # public broker can be slow

import uuid as _uuid
_UNIQUE = _uuid.uuid4().hex[:8]   # avoid topic collisions across parallel runs


@unittest.skipUnless(_RUN_INTEGRATION, "set MAGPIE_INTEGRATION_TESTS=1 to enable")
class TestMqttIntegration(unittest.TestCase):
    """
    End-to-end tests against the free HiveMQ public broker.
    Requires an active internet connection.
    """

    def _make_conn(self, suffix=""):
        from luxai.magpie.transport.mqtt.mqtt_connection import MqttConnection
        conn = MqttConnection(
            _BROKER_URI,
            client_id=f"magpie-test-{_UNIQUE}{suffix}",
        )
        ok = conn.connect(timeout=_BROKER_TIMEOUT)
        self.assertTrue(ok, "Could not connect to HiveMQ public broker")
        return conn

    def test_pubsub_round_trip(self):
        pub_conn = self._make_conn("-pub")
        sub_conn = self._make_conn("-sub")

        topic = f"magpie/ci/{_UNIQUE}/ps"
        sub = MqttSubscriber(sub_conn, topic=topic)
        time.sleep(0.5)    # let subscription propagate

        pub = MqttPublisher(pub_conn)
        pub.write({"hello": "mqtt"}, topic=topic)

        data, recv_topic = sub.read(timeout=_BROKER_TIMEOUT)
        self.assertEqual(data, {"hello": "mqtt"})
        self.assertEqual(recv_topic, topic)

        pub.close()
        sub.close()
        pub_conn.disconnect()
        sub_conn.disconnect()

    def test_rpc_round_trip(self):
        req_conn  = self._make_conn("-req")
        resp_conn = self._make_conn("-resp")
        svc = f"magpie/ci/{_UNIQUE}/rpc"

        responder = MqttRpcResponder(resp_conn, service_name=svc)
        time.sleep(0.5)

        stop = threading.Event()

        def _serve():
            while not stop.is_set():
                try:
                    responder.handle_once(handler=lambda r: {"echo": r}, timeout=0.5)
                except TimeoutError:
                    pass

        t = threading.Thread(target=_serve, daemon=True)
        t.start()

        requester = MqttRpcRequester(req_conn, service_name=svc)
        time.sleep(0.5)

        result = requester.call({"ping": True}, timeout=_BROKER_TIMEOUT)
        self.assertEqual(result, {"echo": {"ping": True}})

        stop.set()
        t.join(timeout=3.0)
        requester.close()
        responder.close()
        req_conn.disconnect()
        resp_conn.disconnect()

    def test_multiple_subscribers_same_topic(self):
        conn1 = self._make_conn("-ms1")
        conn2 = self._make_conn("-ms2")
        conn3 = self._make_conn("-ms3")

        topic = f"magpie/ci/{_UNIQUE}/multi"
        sub1 = MqttSubscriber(conn1, topic=topic)
        sub2 = MqttSubscriber(conn2, topic=topic)
        time.sleep(0.5)

        pub = MqttPublisher(conn3)
        pub.write({"broadcast": 1}, topic=topic)

        d1, _ = sub1.read(timeout=_BROKER_TIMEOUT)
        d2, _ = sub2.read(timeout=_BROKER_TIMEOUT)
        self.assertEqual(d1, {"broadcast": 1})
        self.assertEqual(d2, {"broadcast": 1})

        pub.close()
        sub1.close()
        sub2.close()
        conn1.disconnect()
        conn2.disconnect()
        conn3.disconnect()


if __name__ == "__main__":
    unittest.main()
