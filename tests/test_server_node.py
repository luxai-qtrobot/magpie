import time
import unittest

from magpie.nodes.server_node import ServerNode
from tests.fake_rpc_transports import FakeRpcResponder


class TestServerNode(unittest.TestCase):

    def test_requests_are_handled_and_responses_sent(self):
        # Prepare fake responder with two incoming requests
        responder = FakeRpcResponder(
            recv_items=[
                ({"x": 1}, "CTX1"),
                ({"x": 5}, "CTX2"),
            ]
        )

        # Handler: add 10 to "x"
        def handler(req):
            return {"y": req["x"] + 10}

        node = ServerNode(
            responder=responder,
            handler=handler,
            max_workers=2,
            poll_timeout=0.01,
            paused=False,
        )

        # Let the node run for a short while
        time.sleep(0.1)
        node.terminate()

        # We expect two responses sent back
        self.assertEqual(len(responder.send_calls), 2)
        self.assertIn(({"y": 11}, "CTX1"), responder.send_calls)
        self.assertIn(({"y": 15}, "CTX2"), responder.send_calls)

    def test_timeout_no_requests_results_in_no_responses(self):
        # No incoming requests, always timeout
        responder = FakeRpcResponder(recv_items=[], raise_timeout=True)

        def handler(req):
            # should never be called
            return {"y": 999}

        node = ServerNode(
            responder=responder,
            handler=handler,
            max_workers=1,
            poll_timeout=0.01,
            paused=False,
        )

        time.sleep(0.05)
        node.terminate()

        self.assertEqual(len(responder.send_calls), 0)

    def test_handler_exception_does_not_send_response(self):
        # One incoming request
        responder = FakeRpcResponder(
            recv_items=[
                ({"x": 1}, "CTX1"),
            ]
        )

        def handler(req):
            raise ValueError("boom")

        node = ServerNode(
            responder=responder,
            handler=handler,
            max_workers=1,
            poll_timeout=0.01,
            paused=False,
        )

        time.sleep(0.05)
        node.terminate()

        # No response should be sent if handler fails
        self.assertEqual(len(responder.send_calls), 0)

    def test_terminate_closes_responder(self):
        responder = FakeRpcResponder(recv_items=[])

        def handler(req):
            return req

        node = ServerNode(
            responder=responder,
            handler=handler,
            max_workers=1,
            poll_timeout=0.01,
            paused=False,
        )

        node.terminate()
        self.assertTrue(responder.closed)


if __name__ == "__main__":
    unittest.main()
