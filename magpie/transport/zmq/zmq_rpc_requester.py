from magpie.utils.logger import Logger
from magpie.transport.rpc_requester import RpcRequester
from magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq


class ZMQRpcRequester(RpcRequester):
    """
    ZMQRpcRequester class.

    This class represents an RPC client using a ZeroMQ DEALER socket.
    It serializes request objects, sends them to the ROUTER peer, and
    deserializes responses using the provided serializer.
    """

    def __init__(self,
                 endpoint: str,
                 serializer: MsgpackSerializer = MsgpackSerializer(),
                 name: str = None,
                 identity: bytes = None):
        """
        Initializes the ZMQRpcRequester.

        Args:
            endpoint (str): ZeroMQ endpoint string, e.g.:
                            - "tcp://localhost:5555"
                            - "ipc:///tmp/my_rpc"
                            - "inproc://my_rpc"
            serializer (MsgpackSerializer, optional): Serializer used to
                            convert objects to/from bytes. Defaults to MsgpackSerializer().
            name (str, optional): Name of the requester. Defaults to class name.
            identity (bytes, optional): Optional DEALER identity if you need to
                            distinguish multiple clients at the ROUTER side.
        """
        self.endpoint = endpoint
        self.serializer = serializer

        # Use shared context for inproc, otherwise create a new one
        self.context = zmq.Context.instance() if endpoint.startswith("inproc:") else zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)

        if identity is not None:
            self.socket.setsockopt(zmq.IDENTITY, identity)

        self.socket.connect(endpoint)

        super().__init__(name=name if name is not None else "ZMQRpcRequester")
        Logger.debug(f"{self.name} connected to {self.endpoint} as DEALER.")

    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        """
        Performs the transport-level RPC call via ZeroMQ DEALER.

        Args:
            request_obj (object): Request payload to send.
            timeout (float, optional): Timeout in seconds for waiting for a reply.

        Returns:
            object: Deserialized response object.

        Raises:
            TimeoutError: If no reply is received within the given timeout.
            Exception: For transport-level errors.
        """
        try:
            # Serialize request
            payload = self.serializer.serialize(request_obj)

            # Send single-frame request
            self.socket.send(payload)

            # Blocking receive if no timeout specified
            if timeout is None:
                reply_bytes = self.socket.recv()
            else:
                poller = zmq.Poller()
                poller.register(self.socket, zmq.POLLIN)
                events = dict(poller.poll(int(timeout * 1000)))

                if self.socket not in events or events[self.socket] != zmq.POLLIN:
                    raise TimeoutError(f"{self.name}: RPC call timed out after {timeout} seconds")

                reply_bytes = self.socket.recv()

            # Deserialize response
            return self.serializer.deserialize(reply_bytes)

        except Exception as e:
            Logger.warning(f"{self.name}: transport error during RPC call: {e}")
            raise

    def _transport_close(self) -> None:
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing ZMQ DEALER socket.")
        try:
            self.socket.close()
        except Exception as e:
            Logger.warning(f"{self.name}: error while closing socket: {e}")
