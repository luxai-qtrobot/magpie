import unittest
import time
import socket
import threading

from magpie.transport.zmq.zmq_rpc_requester import ZMQRpcRequester
from magpie.transport.zmq.zmq_rpc_responder import ZMQRpcResponder


def _free_tcp_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    addr, port = s.getsockname()
    s.close()
    return port


def _start_responder_loop(responder: ZMQRpcResponder, handler, stop_event: threading.Event):
    """
    Run responder.handle_once(handler, timeout=0.2) in a loop until stop_event is set.
    """
    def loop():
        while not stop_event.is_set():
            try:
                responder.handle_once(handler=handler, timeout=0.2)
            except TimeoutError:
                # Just try again until stopped
                continue
            except Exception:
                # Any other error: break to avoid noisy loops
                break
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t


class TestZmqRpcTCP(unittest.TestCase):
    """
    TCP tests for ZMQRpcRequester / ZMQRpcResponder
    - A: basic one-shot RPC
    - B1: multiple calls
    - C: timeout behavior
    - D: serialization of complex objects
    - E: clean shutdown
    """

    def test_basic_rpc_tcp(self):
        port = _free_tcp_port()
        endpoint_server = f"tcp://*:{port}"
        endpoint_client = f"tcp://127.0.0.1:{port}"

        responder = ZMQRpcResponder(endpoint=endpoint_server, bind=True)
        time.sleep(0.5)  # let ROUTER bind

        def handler(req):
            return {"result": req["value"] * 2}

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)  # just to be safe

        requester = ZMQRpcRequester(endpoint=endpoint_client)
        time.sleep(0.5)  # allow DEALER connect

        resp = requester.call({"value": 21}, timeout=2.0)
        self.assertEqual(resp, {"result": 42})

        # Clean shutdown
        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()

        self.assertTrue(requester.socket.closed)
        self.assertTrue(responder.socket.closed)

    def test_multiple_calls_tcp(self):
        port = _free_tcp_port()
        endpoint_server = f"tcp://*:{port}"
        endpoint_client = f"tcp://127.0.0.1:{port}"

        responder = ZMQRpcResponder(endpoint=endpoint_server, bind=True)
        time.sleep(0.5)

        def handler(req):
            n = req["n"]
            return {"n": n, "double": 2 * n}

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)

        requester = ZMQRpcRequester(endpoint=endpoint_client)
        time.sleep(0.5)

        for i in range(3):
            resp = requester.call({"n": i}, timeout=2.0)
            self.assertEqual(resp, {"n": i, "double": 2 * i})

        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()

    def test_timeout_tcp(self):
        port = _free_tcp_port()
        endpoint_server = f"tcp://*:{port}"
        endpoint_client = f"tcp://127.0.0.1:{port}"

        # Bind responder but do NOT start any handler loop.
        responder = ZMQRpcResponder(endpoint=endpoint_server, bind=True)
        time.sleep(0.5)  # let ROUTER bind

        requester = ZMQRpcRequester(endpoint=endpoint_client)
        time.sleep(0.5)  # allow DEALER connect

        try:
            with self.assertRaises(TimeoutError):
                requester.call({"x": 1}, timeout=0.5)
        finally:
            requester.close()
            responder.close()


    def test_serialization_complex_object_tcp(self):
        port = _free_tcp_port()
        endpoint_server = f"tcp://*:{port}"
        endpoint_client = f"tcp://127.0.0.1:{port}"

        responder = ZMQRpcResponder(endpoint=endpoint_server, bind=True)
        time.sleep(0.5)

        def handler(req):
            # Echo back with some modification
            req = dict(req)
            req["ok"] = True
            return req

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)

        requester = ZMQRpcRequester(endpoint=endpoint_client)
        time.sleep(0.5)

        payload = {
            "num": 123,
            "list": [1, 2, 3],
            "nested": {"a": "b", "c": [4, 5]},
        }

        resp = requester.call(payload, timeout=2.0)

        self.assertEqual(resp["num"], 123)
        self.assertEqual(resp["list"], [1, 2, 3])
        self.assertEqual(resp["nested"], {"a": "b", "c": [4, 5]})
        self.assertTrue(resp["ok"])

        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()


class TestZmqRpcInproc(unittest.TestCase):
    """
    Same tests as above but using inproc:// endpoints.
    For inproc, responder (ROUTER) must bind first, then requester connects.
    """

    def test_basic_rpc_inproc(self):
        endpoint = "inproc://rpc-basic"

        responder = ZMQRpcResponder(endpoint=endpoint, bind=True)
        time.sleep(0.5)

        def handler(req):
            return {"sum": req["a"] + req["b"]}

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)

        requester = ZMQRpcRequester(endpoint=endpoint)
        time.sleep(0.5)

        resp = requester.call({"a": 10, "b": 32}, timeout=2.0)
        self.assertEqual(resp, {"sum": 42})

        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()

    def test_multiple_calls_inproc(self):
        endpoint = "inproc://rpc-multi"

        responder = ZMQRpcResponder(endpoint=endpoint, bind=True)
        time.sleep(0.5)

        def handler(req):
            v = req["v"]
            return {"v": v, "square": v * v}

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)

        requester = ZMQRpcRequester(endpoint=endpoint)
        time.sleep(0.5)

        for v in [2, 3, 4]:
            resp = requester.call({"v": v}, timeout=2.0)
            self.assertEqual(resp, {"v": v, "square": v * v})

        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()

    def test_timeout_inproc(self):
        # No responder bound to this endpoint
        endpoint = "inproc://rpc-no-server"

        requester = ZMQRpcRequester(endpoint=endpoint)
        time.sleep(0.5)

        with self.assertRaises(TimeoutError):
            requester.call({"foo": "bar"}, timeout=0.5)

        requester.close()

    def test_serialization_complex_object_inproc(self):
        endpoint = "inproc://rpc-complex"

        responder = ZMQRpcResponder(endpoint=endpoint, bind=True)
        time.sleep(0.5)

        def handler(req):
            return {"wrapped": req, "len_list": len(req.get("items", []))}

        stop_event = threading.Event()
        thread = _start_responder_loop(responder, handler, stop_event)

        time.sleep(0.5)

        requester = ZMQRpcRequester(endpoint=endpoint)
        time.sleep(0.5)

        payload = {"items": ["a", "b", "c"], "flag": True}
        resp = requester.call(payload, timeout=2.0)

        self.assertEqual(resp["wrapped"], payload)
        self.assertEqual(resp["len_list"], 3)

        requester.close()
        stop_event.set()
        thread.join(timeout=2.0)
        responder.close()


if __name__ == "__main__":
    unittest.main()
