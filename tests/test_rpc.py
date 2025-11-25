import unittest

from tests.fake_rpc_transports import FakeRpcRequester, FakeRpcResponder


# ---------------------------------------------------
# RpcRequester tests
# ---------------------------------------------------

class TestRpcRequester(unittest.TestCase):

    def test_call_success(self):
        requester = FakeRpcRequester(response="OK")
        result = requester.call({"x": 1}, timeout=2)

        self.assertEqual(result, "OK")
        self.assertEqual(requester.calls[0], ({"x": 1}, 2))

    def test_call_raises_and_propagates(self):
        requester = FakeRpcRequester(raise_exc=ValueError("bad"))

        with self.assertRaises(ValueError):
            requester.call({"y": 2})

        self.assertEqual(len(requester.calls), 1)

    def test_close_calls_transport_close(self):
        requester = FakeRpcRequester()
        requester.close()
        self.assertTrue(requester.closed)


# ---------------------------------------------------
# RpcResponder tests
# ---------------------------------------------------

class TestRpcResponder(unittest.TestCase):

    def test_handle_once_success(self):
        # recv_items: list of (request_obj, client_ctx)
        responder = FakeRpcResponder(recv_items=[
            ({"a": 1}, "CTX1")
        ])

        def handler(req):
            return {"resp": req["a"] + 10}

        handled = responder.handle_once(handler)

        self.assertTrue(handled)
        self.assertEqual(
            responder.send_calls[0],
            ({"resp": 11}, "CTX1")
        )

    def test_handle_once_timeout_returns_false(self):
        responder = FakeRpcResponder(raise_timeout=True)

        def handler(req):
            return 123  # should never be called

        handled = responder.handle_once(handler)

        self.assertFalse(handled)
        self.assertEqual(len(responder.send_calls), 0)

    def test_close_calls_transport_close(self):
        responder = FakeRpcResponder()
        responder.close()
        self.assertTrue(responder.closed)


if __name__ == "__main__":
    unittest.main()
