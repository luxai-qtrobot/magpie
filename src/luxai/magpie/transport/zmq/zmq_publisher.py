
from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.utils.logger import Logger
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq
import zmq.utils.monitor
import time

class ZMQPublisher(StreamWriter):
    """
    ZMQPublisher class.
    
    This class is responsible for publishing messages to a ZeroMQ socket. 
    It uses the PUB socket type, which is typically used in the Publisher-Subscriber 
    pattern, where the publisher sends messages to all connected subscribers.
    """

    def __init__(
        self,
        endpoint: str,
        serializer=MsgpackSerializer(),
        queue_size: int = 10,
        bind: bool = True,
        delivery: str = "reliable",     # "reliable" or "latest"
    ):
        """
        Args:
            endpoint (str): ZeroMQ endpoint (tcp://*, inproc://x, ipc://...).
            serializer (MsgpackSerializer): Serializer for outgoing messages.
            queue_size (int): Size of internal writer queue.
            bind (bool): Whether to bind() or connect().
            delivery (str): High-level delivery mode:
                            - "reliable": default PUB behaviour
                            - "latest": optimized for real-time streams
        """
        self.endpoint = endpoint
        self.serializer = serializer
        self.delivery = delivery
        self.bind = bind

        # Use shared context for inproc, otherwise a new context
        self.context = zmq.Context.instance() if endpoint.startswith('inproc:') else zmq.Context()
        self.socket = self.context.socket(zmq.PUB)

        # Apply delivery mode for publisher side
        if self.delivery == "latest":
            # Only sender side option needed for real-time semantics:
            # Prevent large outbound queue buildup
            self.socket.setsockopt(zmq.SNDHWM, 1)

        # Bind or connect
        if bind:
            self.socket.bind(endpoint)
            action = "bound"
            self._monitor = None
        else:
            # Attach monitor BEFORE connect so EVENT_CONNECTED is never missed
            self._monitor = self.socket.get_monitor_socket()
            self._monitor.setsockopt(zmq.RCVTIMEO, 100)
            self.socket.connect(endpoint)
            action = "connected"

        # Start StreamWriter worker thread
        super().__init__(name='ZMQPublisher', queue_size=queue_size)

        Logger.debug(
            f"ZMQPublisher is ready ({action} at {self.endpoint}, delivery={self.delivery})"
        )

    def _transport_write(self, data: object, topic: str):
        """
        Publishes a message to the ZeroMQ socket with an optional topic.

        Args:
            data (object): The data object to be serialized and sent.
            topic (str, optional): The topic under which the data is published.
        """
        try:
            topic = '' if not topic else topic
            topic_bytes = topic.encode()
            payload = self.serializer.serialize(data)  # must return a fresh bytes-like buffer

            # Zero-copy send where possible
            self.socket.send_multipart([topic_bytes, memoryview(payload)], copy=False)
        except Exception as e:
            Logger.warning(f"{self.name} write failed with: {str(e)}")

    def _transport_close(self):
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing.")

        try:
            self.socket.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name} socket close error: {e}")

        # Close context only if this publisher created it
        try:
            if not self.endpoint.startswith("inproc:"):
                self.context.term()
        except Exception as e:
            Logger.warning(f"{self.name} context close error: {e}")

    def wait_connect(self, timeout: float = None) -> bool:
        if self.bind or self._monitor is None:
            return True

        connected = False
        start_t = time.time()
        try:
            while not self._closed:
                if timeout is not None and (time.time() - start_t) > timeout:
                    Logger.debug(f"ZMQPublisher: wait_connect timed out after {timeout}s (endpoint={self.endpoint})")
                    break
                try:
                    evt = zmq.utils.monitor.recv_monitor_message(self._monitor)
                    if evt['event'] == zmq.EVENT_CONNECTED:
                        connected = True
                        time.sleep(0.1)  # small delay to ensure connection is fully established
                        break
                except zmq.Again:
                    continue
        finally:
            self.socket.disable_monitor()
            self._monitor = None

        return connected