from magpie.transport.stream_writer import StreamWriter
from magpie.utils.logger import Logger
from magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import ZMQContext, zmq

class ZMQPublisher(StreamWriter):
    """
    ZMQPublisher class.
    
    This class is responsible for publishing messages to a ZeroMQ socket. 
    It uses the PUB socket type, which is typically used in the Publisher-Subscriber 
    pattern, where the publisher sends messages to all connected subscribers.
    """

    def __init__(self, endpoint: str, serializer=MsgpackSerializer(), queue_size=1):
        """
        Initializes the ZMQPublisher class.

        Args:
            endpoint (str): The ZeroMQ endpoint is a string consisting of a <transport>://<address>. 
                            The transport specifies the underlying protocol to use such as 'tcp', 'ipc', or 'inproc'. 
                            The address specifies the transport-specific address to bind to.
                            - tcp example:     tcp://*:5555
                            - inproc example:  inproc://my_publisher
                            - ipc example:     ipc:///tmp/my_publisher
            serializer (MsgpackSerializer, optional): The serializer used to convert data into byte format before sending. 
                                                      Defaults to `MsgpackSerializer`.
        """
        self.endpoint = endpoint  # Corrected typo from 'endpint' to 'endpoint'
        self.serializer = serializer
        # Use a shared ZMQ context if the endpoint is 'inproc', otherwise create a new context
        self.context = ZMQContext.get_instance() if endpoint.startswith('inproc:') else zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(endpoint)
        super().__init__(name='ZMQPublisher', queue_size=queue_size)
        Logger.debug(f"{self.name} is ready")

    def _transport_write(self, data: object, topic=''):
        """
        Publishes a message to the ZeroMQ socket with an optional topic.

        Args:
            data (object): The data object to be serialized and sent.
            topic (str, optional): The topic under which the data is published. Defaults to an empty string.
        
        """
        try:
            # Send the topic and serialized data as multipart
            self.socket.send_multipart([topic.encode(), self.serializer.serialize(data)])
        except Exception as e:
            Logger.warning(f"{self.name} write failed with: {str(e)}")
        
    def _transport_close(self):
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        self.socket.close()
        # Optional: self.context.term() to terminate the context if it's no longer needed

    def __del__(self):
        """
        Destructor to ensure that the socket is closed and resources are cleaned up when the object is deleted.
        """
        self._transport_close()
        Logger.debug(f"{self.name} is terminated.")
