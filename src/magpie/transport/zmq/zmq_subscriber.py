from typing import Union, List
from magpie.transport.stream_reader import StreamReader
from magpie.utils.logger import Logger
from magpie.serializer.msgpack_serializer import MsgpackSerializer
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
            endpoint (str): The ZeroMQ endpoint is a string consisting of a <transport>://<address>. 
                            The transport specifies the underlying protocol to use such as 'tcp', 'ipc', or 'inproc'. 
                            The address specifies the transport-specific address to connect to.
                            - tcp example:     tcp://*:5555
                            - inproc example:  inproc://my_publisher
                            - ipc example:     ipc:///tmp/my_publisher
            topic (str or list, optional): The topic(s) to subscribe to. Empty string subscribes to all topics.
            serializer (MsgpackSerializer, optional): The serializer used to convert byte data back into objects. 
            queue_size (int, optional): Max size of the internal StreamReader queue.
            bind (bool, optional): If True, SUB socket will bind() to endpoint; otherwise it will connect().
            delivery (str, optional): High-level delivery mode:
                                      - "reliable": default ZeroMQ behaviour.
                                      - "latest": tuned for real-time streams (e.g. video).
        """
        self.endpoint = endpoint
        self.serializer = serializer
        self.delivery = delivery

        # Set up the subscription topics
        topic = '' if topic is None else topic
        if isinstance(topic, (list, tuple)):
            self.topics = list(topic)
        else:
            self.topics = [topic]

        # Use a shared ZMQ context if the endpoint is 'inproc', otherwise create a new context
        self.context = zmq.Context.instance() if endpoint.startswith('inproc:') else zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

        # Apply delivery mode defaults
        if self.delivery == "latest":
            # Optimised for "always latest" semantics (e.g. video frames)
            self.socket.setsockopt(zmq.RCVHWM, 1)            

        # Bind or connect
        if bind:
            self.socket.bind(endpoint)
            action = "bound"
        else:
            self.socket.connect(endpoint)
            action = "connected"

        # Set the subscription topic; empty string subscribes to all topics
        if any(t == '' for t in self.topics):
            self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        else:
            for t in self.topics:
                self.socket.setsockopt(zmq.SUBSCRIBE, t.encode())

        super().__init__(name='ZMQSubscriber', queue_size=queue_size)
        Logger.debug(
            f"ZMQSubscriber is ready ({action} at {self.endpoint} "
            f"for topics: {self.topics}, delivery={self.delivery}, queue_size={self.queue_size})"
        )

    def _transport_read_blocking(self) -> (object, str):
        """
        Reads a message and topic  from the ZeroMQ socket using a poller with timeout.        
        - If _transport_close() has been called (socket/context closed),
          this returns None and stops blocking.
        """
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while True:
            # If socket/context already closed, just exit
            if self.socket.closed or self.context.closed:
                Logger.debug(f"{self.name}: socket/context closed, stop reading.")
                return None

            try:                
                events = dict(poller.poll(1000))
            except zmq.ZMQError as e:                
                if self.socket.closed or self.context.closed:                    
                    return None
                # Otherwise, let the caller deal with real ZMQ errors
                raise

            if self.socket in events and (events[self.socket] & zmq.POLLIN):
                # Socket is readable; recv and deserialize
                topic, msg = self.socket.recv_multipart()
                return self.serializer.deserialize(msg), topic.decode()

    def _transport_close(self): 
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing.")
        self.socket.close()        

    def __del__(self):        
        """
        Destructor to ensure that the socket is closed and resources are cleaned up when the object is deleted.
        """
        self.socket.close()        
        Logger.debug(f"ZMQSubscriber is terminated.")
