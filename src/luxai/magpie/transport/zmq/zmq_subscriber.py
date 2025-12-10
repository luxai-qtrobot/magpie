import time
from typing import Union, List, Tuple
from luxai.magpie.transport.stream_reader import StreamReader
from luxai.magpie.utils.logger import Logger
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq


class ZMQSubscriber(StreamReader):
    """
    ZMQSubscriber class.
    
    This class represents a subscriber in a ZeroMQ publish-subscribe pattern. 
    It listens to a specified endpoint and topic for incoming messages, 
    which are then deserialized using the specified serializer.
    """

    def __init__(
        self,
        endpoint: str,
        topic: Union[str, List[str]] = '',
        serializer=MsgpackSerializer(),
        queue_size: int = 10,
        bind: bool = False,
        delivery: str = "reliable",   # "reliable" or "latest"
    ):
        """
        Initializes the ZMQSubscriber class.

        Args:
            endpoint (str): ZeroMQ endpoint (tcp://*, inproc://name, ipc://...).
            topic (str or list): Topic(s) to subscribe to. Empty string subscribes to all topics.
            serializer (MsgpackSerializer): Serializer for converting bytes to objects.
            queue_size (int): Size of the internal StreamReader queue.
            bind (bool): Whether SUB socket should bind() or connect().            
            delivery (str): High-level delivery mode:
                                - "reliable": default ZeroMQ behaviour.
                                - "latest": tuned for real-time streams (e.g. video)            
        """
        self.endpoint = endpoint
        self.serializer = serializer
        self.delivery = delivery

        # Normalize topics
        topic = '' if topic is None else topic
        if isinstance(topic, (list, tuple)):
            self.topics = list(topic)
        else:
            self.topics = [topic]

        # Context choice
        self.context = zmq.Context.instance() if endpoint.startswith('inproc:') else zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

        # "latest" mode optimization
        if self.delivery == "latest":
            self.socket.setsockopt(zmq.RCVHWM, 1)

        # Bind or connect
        if bind:
            self.socket.bind(endpoint)
            action = "bound"
        else:
            self.socket.connect(endpoint)
            action = "connected"

        # Subscription setup
        if any(t == '' for t in self.topics):
            self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        else:
            for t in self.topics:
                self.socket.setsockopt(zmq.SUBSCRIBE, t.encode())

        # Start StreamReader background reader thread
        super().__init__(name='ZMQSubscriber', queue_size=queue_size)

        Logger.debug(
            f"ZMQSubscriber is ready ({action} at {self.endpoint} "
            f"for topics: {self.topics}, delivery={self.delivery}, queue_size={self.queue_size})"
        )

    def _transport_read_blocking(self, timeout: float = None) -> Tuple[object, str]:
        """
        Reads a message and topic from the ZeroMQ socket using a poller with timeout.

        - If the transport has been closed, return None to signal the read loop to stop.
        - Uses poll() in chunks of up to 1 second so reader_close_event can interrupt promptly.
        """
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        start_t = time.time()

        while True:
            # If socket/context already closed, just exit
            if self.socket.closed or self.context.closed:
                Logger.debug(f"{self.name}: socket/context closed, stop reading.")
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
                topic, msg = self.socket.recv_multipart()
                return self.serializer.deserialize(msg), topic.decode()

            # Timeout reached?
            if timeout is not None and (time.time() - start_t) > timeout:
                raise TimeoutError(f"{self.name}: no data received within {timeout} seconds")

    def _transport_close(self):
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing.")

        # Close socket first
        try:
            self.socket.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name} socket close error: {e}")

        # Terminate context if we created it
        try:
            if not self.endpoint.startswith("inproc:"):
                self.context.term()
        except Exception as e:
            Logger.warning(f"{self.name} context close error: {e}")
