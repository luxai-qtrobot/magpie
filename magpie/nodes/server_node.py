from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from magpie.nodes.base_node import BaseNode
from magpie.transport.rpc_responder import RpcResponder, RpcHandlerType
from magpie.utils.logger import Logger


class ServerNode(BaseNode):
    """
    ServerNode

    A node that:
      - receives RPC requests via an RpcResponder
      - dispatches each request to a handler in a worker thread
      - sends responses back from the BaseNode thread

    This allows multiple concurrent in-flight requests while
    keeping all ZeroMQ I/O in a single thread.
    """

    def __init__(self,
                 responder: RpcResponder,
                 handler: RpcHandlerType,
                 max_workers: int = 4,
                 poll_timeout: float = 0.01,
                 name: str = None,
                 paused: bool = False,
                 setup_kwargs: dict = {}):
        """
        Initializes the ServerNode.

        Args:
            responder (RpcResponder): Transport-level responder (e.g., ZMQRpcResponder).
            handler (callable): Function with signature handler(request_obj) -> response_obj.
            max_workers (int): Maximum number of worker threads for handling requests.
            poll_timeout (float): Timeout in seconds used when waiting for new requests.
            name (str, optional): Name of the node.
            paused (bool, optional): Start the node in paused state.
            setup_kwargs (dict, optional): Extra kwargs passed to setup().
        """
        self.responder = responder
        self.handler = handler
        self.max_workers = max_workers
        self.poll_timeout = poll_timeout

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.reply_queue = Queue()

        super().__init__(name=name, paused=paused, setup_kwargs=setup_kwargs)

    def setup(self):
        """No extra setup for now, kept for symmetry with BaseNode."""
        pass

    def _dispatch_request(self, request_obj: object, client_ctx: object) -> None:
        """
        Submits the handler work to the executor.
        The worker thread will put the (response_obj, client_ctx) into reply_queue.
        """
        def job():
            try:
                response_obj = self.handler(request_obj)
            except Exception as e:
                Logger.warning(f"{self.name}: handler error: {e}")
                # You might want to send an error response here instead.
                return

            try:
                self.reply_queue.put((response_obj, client_ctx))
            except Exception as e:
                Logger.warning(f"{self.name}: failed to enqueue response: {e}")

        self.executor.submit(job)

    def _drain_replies(self) -> None:
        """
        Sends all ready responses from the reply_queue using the responder.
        Must be called only from the BaseNode thread.
        """
        while True:
            try:
                response_obj, client_ctx = self.reply_queue.get_nowait()
            except Empty:
                break

            try:
                # Use public or protected send method from RpcResponder.
                # If you add a public send_response(), call that instead.
                self.responder._transport_send(response_obj, client_ctx)
            except Exception as e:
                Logger.warning(f"{self.name}: error sending response: {e}")

    def process(self):
        """
        Single iteration of the node's main loop, called repeatedly by BaseNode._run().
        It:
          1. Sends any queued responses.
          2. Tries to receive one new request with a small timeout and dispatch it.
        """
        # 1) Send any ready responses first (keep latency low)
        self._drain_replies()

        # 2) Try to receive a single new request (non-blocking-ish)
        try:
            request_obj, client_ctx = self.responder._transport_recv(timeout=self.poll_timeout)
        except TimeoutError:
            # No request within poll_timeout; just return, BaseNode will call process() again.
            return
        except Exception as e:
            Logger.warning(f"{self.name}: error receiving request: {e}")
            return

        # 3) Dispatch the request to worker pool
        self._dispatch_request(request_obj, client_ctx)

    def interrupt(self):
        """Called when terminate() is requested; can be used to wake things up if needed."""
        pass

    def cleanup(self):
        """
        Called after the thread loop exits.
        Ensure the executor and responder are closed.
        """
        try:
            self.executor.shutdown(wait=True)
        except Exception as e:
            Logger.warning(f"{self.name}: error shutting down executor: {e}")

        try:
            self.responder.close()
        except Exception as e:
            Logger.warning(f"{self.name}: error closing responder: {e}")

    def terminate(self, timeout: float = None):
        """
        Terminates the ServerNode:
          - shuts down executor
          - closes responder
          - stops the BaseNode thread
        """
        # First stop the BaseNode loop (this will eventually call cleanup())
        super().terminate(timeout)
