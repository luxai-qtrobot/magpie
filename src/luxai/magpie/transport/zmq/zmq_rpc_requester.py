import time
from luxai.magpie.utils.logger import Logger
from luxai.magpie.transport.rpc_requester import RpcRequester
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
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
        except Exception as e:
            Logger.warning(f"{self.name}: transport error during RPC call: {e}")
            raise

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        start_t = time.time()
        while True:
            # If socket/context already closed, just exit
            if self.socket.closed or self.context.closed:
                Logger.debug(f"{self.name}: socket/context closed, stop reading.")
                return None

            try:
                poller_timeout = 1000 if not timeout else min(timeout*1000, 1000)
                events = dict(poller.poll(poller_timeout))
            except zmq.ZMQError as e:                
                if self.socket.closed or self.context.closed:              
                    return None
                # Otherwise, let the caller deal with real ZMQ errors
                Logger.warning(f"{self.name}: transport error during recv: {e}")
                raise

            if self.socket in events and (events[self.socket] & zmq.POLLIN):
                reply_bytes = self.socket.recv()
                return self.serializer.deserialize(reply_bytes)

            # check if timeout occured 
            if timeout and (time.time() - start_t) > timeout:
                raise TimeoutError(f"{self.name}: no request received within {timeout} seconds")


    def _transport_close(self) -> None:
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing ZMQ DEALER socket.")
        self.socket.close()

    def __del__(self):        
        """
        Destructor to ensure that the socket is closed and resources are cleaned up when the object is deleted.
        """
        self.socket.close()
        Logger.debug(f"ZMQRpcRequester is terminated.")
