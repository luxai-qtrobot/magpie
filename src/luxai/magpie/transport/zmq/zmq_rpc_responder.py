import time
from luxai.magpie.utils.logger import Logger
from luxai.magpie.transport.rpc_responder import RpcResponder
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq


class ZMQRpcResponder(RpcResponder):
    """
    ZMQRpcResponder class.

    This class represents an RPC server using a ZeroMQ ROUTER socket.
    It receives requests from DEALER clients, deserializes them using the
    provided serializer, calls a handler (via RpcResponder.handle_once),
    and sends back serialized responses.
    """

    def __init__(
        self,
        endpoint: str,
        serializer=None,
        name: str = None,
        bind: bool = True,
        schema=None,
    ):
        """
        Initializes the ZMQRpcResponder.

        Args:
            endpoint (str): ZeroMQ endpoint string, e.g.:
                            - "tcp://*:5555"
                            - "ipc:///tmp/my_rpc"
                            - "inproc://my_rpc"
            serializer (MsgpackSerializer, optional): Serializer used to convert
                            objects to/from bytes. Defaults to MsgpackSerializer().
            name (str, optional): Name of the responder. Defaults to class name.
            bind (bool, optional): If True, ROUTER will bind() to endpoint.
                                   If False, it will connect() instead.
        """
        self.endpoint = endpoint
        self.serializer = serializer or MsgpackSerializer()

        # Use shared context for inproc, otherwise create a new one
        self.context = zmq.Context.instance() if endpoint.startswith("inproc:") else zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)

        if bind:
            self.socket.bind(endpoint)
            action = "bound"
        else:
            self.socket.connect(endpoint)
            action = "connected"

        super().__init__(name=name if name is not None else "ZMQRpcResponder", schema=schema)
        Logger.debug(f"{self.name} {action} ROUTER at {self.endpoint}.")

    def _transport_recv(self, timeout: float = None) -> tuple:
        """
        Receives a single request via ZeroMQ ROUTER.

        Args:
            timeout (float, optional): Timeout in seconds for waiting for a request.

        Returns:
            tuple: (request_obj, client_identity)

        Raises:
            TimeoutError: If no request is received within the timeout.
            Exception: For transport-level errors.
        """
        # wait for request with timeout
        request_obj, client_identity = self._socket_receive(timeout=timeout)

        # check request validity. it should be like this: {"rid": ..., "payload": ...}
        if request_obj is None or "rid" not in request_obj or "payload" not in request_obj:
            raise RuntimeError(f"{self.name}: invalid request format: {request_obj}")

        # Build client_ctx containing identity + request id
        client_ctx = {
            "identity": client_identity,
            "rid": request_obj["rid"],
        }

        # send ack back to client: {"rid": request_obj["rid"], "ack": true}
        # we can not use _transport_send here because we need to send different format
        # our ack format is: {"rid": request_obj["rid"], "ack": true} 
        # lets use raw socket send try/except to catch any transport errors 
        try:
            ack_payload = self.serializer.serialize({"rid": request_obj["rid"], "ack": True})
            self.socket.send_multipart([client_identity, ack_payload])
        except zmq.ZMQError as e:
            Logger.warning(f"{self.name}: transport error during ack send: {e}")
            raise

        return request_obj["payload"], client_ctx


    def _socket_receive(self, timeout: float = None) -> object:
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        start_t = time.time()
        while True:
            # If socket/context already closed, just exit
            if self.socket.closed:
                Logger.debug(f"{self.name}: socket closed, stop reading.")
                return None, None

            try:
                poll_ms = 1000 if timeout is None else min(timeout * 1000, 1000)
                events = dict(poller.poll(poll_ms))
            except zmq.ZMQError as e:
                if self.socket.closed:
                    return None, None
                Logger.warning(f"{self.name}: transport error during recv: {e}")
                raise

            if self.socket in events and (events[self.socket] & zmq.POLLIN):
                frames = self.socket.recv_multipart()
                if len(frames) < 2:
                    raise RuntimeError(f"{self.name}: invalid message format, expected [identity, payload]")
                client_identity = frames[0]
                payload = frames[-1]
                request_obj = self.serializer.deserialize(payload)
                return request_obj, client_identity

            # check if timeout occured 
            if timeout is not None and (time.time() - start_t) > timeout:
                raise TimeoutError(f"{self.name}: no request received within {timeout} seconds")

    def _transport_send(self, response_obj: object, client_ctx: object) -> None:
        """
        Sends the response back to the client via ZeroMQ ROUTER.

        Args:
            response_obj (object): The response payload.
            client_ctx (object): The client identity (bytes) from _transport_recv().
        """
        try:
            # we need to make correct response format: {"rid": client_ctx["rid"], "payload": response_obj}
            response_obj = {"rid": client_ctx["rid"], "payload": response_obj}
            payload = self.serializer.serialize(response_obj)
            # ROUTER requires identity frame first
            self.socket.send_multipart([client_ctx["identity"], payload])
        except Exception as e:
            Logger.warning(f"{self.name}: transport error during send: {e}")
            raise

    def _transport_close(self) -> None:
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing ZMQ ROUTER socket.")

        # Close socket immediately
        try:
            self.socket.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: socket close error: {e}")

        # Terminate context if we created it
        try:
            if not self.endpoint.startswith("inproc:"):
                self.context.term()
        except Exception as e:
            Logger.warning(f"{self.name}: context close error: {e}")
