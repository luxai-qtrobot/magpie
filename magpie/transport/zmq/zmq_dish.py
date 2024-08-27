from magpie.transport.stream_reader import StreamReader
from magpie.utils.logger import Logger
from magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq

class ZMQDish(StreamReader):
    """
    ZMQDish class.
    
    """

    def __init__(self, endpoint: str, serializer=MsgpackSerializer(), queue_size=1):
        """
        Initializes the ZMQDish class.

        Args:
            endpoint (str): The ZeroMQ endpoint is a string consisting of a <transport>://<address>. 
                            The transport specifies the underlying protocol to use such as 'udp'. 
                            The address specifies the transport-specific address to connect to.
                            - udp example:     udp://*:5555
            serializer (MsgpackSerializer, optional): The serializer used to convert byte data back into objects. 
                                                      Defaults to `MsgpackSerializer`.
        """
        self.endpoint = endpoint
        self.serializer = serializer
        # Use a shared ZMQ context if the endpoint is 'inproc', otherwise create a new context
        self.context = zmq.Context.instance() if endpoint.startswith('inproc:') else zmq.Context()
        self.socket = self.context.socket(zmq.DISH)
        self.socket.bind(endpoint)
        self.socket.join("*")
        # Set the subscription topic; empty string subscribes to all topics        
        super().__init__(name='ZMQDish', queue_size=queue_size)
        Logger.debug(f"ZMQDish is ready")

    def _transport_read_blocking(self) -> object:
        """
        Reads a message from the ZeroMQ socket, deserializes it, and returns the corresponding object.

        Returns:
            object: The deserialized data object received from the publisher, or None if an error occurs.        
        """
        try:
            # Receive a multipart message; expect topic and message parts
            msg = self.socket.recv(copy=False)
            # Logger.debug(f"{self.name} received {len(msg)} bytes from topic '{topic.decode('utf-8')}'.")
            return self.serializer.deserialize(msg)
        except Exception as e:
            Logger.debug(f"{self.name} encountered an error: {str(e)}")
        return None

    def _transport_close(self): 
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"ZMQDish is closing.")
        self.socket.close()
        # Optional: self.context.term() to terminate the context if it's no longer needed

    def __del__(self):        
        """
        Destructor to ensure that the socket is closed and resources are cleaned up when the object is deleted.
        """
        self._transport_close()
        Logger.debug(f"ZMQDish is terminated.")
