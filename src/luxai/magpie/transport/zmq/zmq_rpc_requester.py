import time
from luxai.magpie.utils.logger import Logger
from luxai.magpie.transport.rpc_requester import RpcRequester, AckTimeoutError, ReplyTimeoutError
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.common import get_uinque_id
from .zmq_utils import zmq


class ZMQRpcRequester(RpcRequester):
    """
    ZMQRpcRequester class.

    This class represents an RPC client using a ZeroMQ DEALER socket.
    It serializes request objects, sends them to the ROUTER peer, and
    deserializes responses using the provided serializer.
    """

    def __init__(
        self,
        endpoint: str,
        serializer: MsgpackSerializer = MsgpackSerializer(),
        name: str = None,
        identity: bytes = None,
        ack_timeout: float = 2.0
    ):
        """
        Initializes the ZMQRpcRequester.
        """
        self.endpoint = endpoint
        self.serializer = serializer
        self.ack_timeout = ack_timeout

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
        """
        # ---- Send request ----
        try:
            req = {
                "rid": get_uinque_id(),
                "payload": request_obj,
            }
            payload = self.serializer.serialize(req)
            self.socket.send(payload)
        except Exception as e:
            Logger.warning(f"{self.name}: transport error during RPC call: {e}")
            raise

        # ---- Wait for ACK ----
        try:
            ack_timeout = min(timeout, self.ack_timeout) if timeout else self.ack_timeout
            ack = self._socket_receive(timeout=ack_timeout)
        except TimeoutError:
            raise AckTimeoutError(f"{self.name}: no ack received within {ack_timeout} seconds")
        except Exception as e:
            Logger.warning(f"{self.name}: transport error during ack receive: {e}")
            raise

        if ack is None or ack.get("rid") != req["rid"] or not ack.get("ack", False):
            raise RuntimeError(f"{self.name}: invalid ack received: {ack}")

        # ---- Wait for reply ----
        try:
            reply = self._socket_receive(timeout=timeout)
        except TimeoutError:
            raise ReplyTimeoutError(f"{self.name}: no reply received within {timeout} seconds")
        except Exception as e:
            Logger.warning(f"{self.name}: transport error during reply receive: {e}")
            raise

        if reply is None or reply.get("rid") != req["rid"] or "payload" not in reply:
            raise RuntimeError(f"{self.name}: invalid reply received: {reply}")

        return reply["payload"]

    def _socket_receive(self, timeout: float = None) -> object:
        """
        Polls the DEALER socket for incoming frames, deserializes them, and returns the object.
        """
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        start_t = time.time()
        while True:
            if self.socket.closed:
                Logger.debug(f"{self.name}: socket closed, stop reading.")
                return None

            try:
                poll_ms = 1000 if timeout is None else min(timeout * 1000, 1000)
                events = dict(poller.poll(poll_ms))
            except zmq.ZMQError as e:
                if self.socket.closed:
                    return None
                Logger.warning(f"{self.name}: transport error during recv: {e}")
                raise

            if self.socket in events and (events[self.socket] & zmq.POLLIN):
                reply_bytes = self.socket.recv()
                return self.serializer.deserialize(reply_bytes)

            if timeout is not None and (time.time() - start_t) > timeout:
                raise TimeoutError(f"{self.name}: no response received within {timeout} seconds")

    def _transport_close(self) -> None:
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """

        Logger.debug(f"{self.name} is closing ZMQ DEALER socket.")

        # Close socket immediately without waiting for peer
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
