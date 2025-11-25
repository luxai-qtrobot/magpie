from magpie.transport.rpc_requester import RpcRequester
from magpie.transport.rpc_responder import RpcResponder

class FakeRpcRequester(RpcRequester):
    """
    A fake RPC requester for testing.
    Records calls to _transport_call and returns a preset response.
    """

    def __init__(self, response=None, raise_exc=None):
        super().__init__()
        self.response = response
        self.raise_exc = raise_exc
        self.calls = []
        self.closed = False

    def _transport_call(self, request_obj, timeout=None):
        self.calls.append((request_obj, timeout))

        if self.raise_exc:
            raise self.raise_exc

        return self.response

    def _transport_close(self):
        self.closed = True


class FakeRpcResponder(RpcResponder):
    def __init__(self, recv_items=None, raise_timeout=False):
        super().__init__()
        self.recv_items = recv_items or []
        self.send_calls = []
        self.raise_timeout = raise_timeout
        self.closed = False
        self.recv_index = 0

    def _transport_recv(self, timeout=None):
        if self.raise_timeout:
            raise TimeoutError("No request")

        if self.recv_index >= len(self.recv_items):
            raise TimeoutError("No request")

        item = self.recv_items[self.recv_index]
        self.recv_index += 1
        return item  # (request_obj, ctx)

    def _transport_send(self, response_obj, client_ctx):
        self.send_calls.append((response_obj, client_ctx))

    def _transport_close(self):
        self.closed = True
