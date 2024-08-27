from magpie.transport.stream_writer import StreamWriter
from magpie.utils.logger import Logger
from magpie.serializer.msgpack_serializer import MsgpackSerializer
from .zmq_utils import zmq

class ZMQRadio(StreamWriter):
    """
    ZMQRadio class.
    
    """

    def __init__(self, endpoint: str, serializer=MsgpackSerializer(), queue_size=1):
        """
        Initializes the ZMQPublisher class.

        Args:
            endpoint (str): The ZeroMQ endpoint is a string consisting of a <transport>://<address>. 
                            The transport specifies the underlying protocol to use such as 'udp'. 
                            The address specifies the transport-specific address to bind to.
                            - udp example:     udp://127.0.0.1:5555
            serializer (MsgpackSerializer, optional): The serializer used to convert data into byte format before sending. 
                                                      Defaults to `MsgpackSerializer`.
        """
        self.endpoint = endpoint
        self.serializer = serializer        
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.RADIO)
        self.socket.connect(endpoint)
        super().__init__(name='ZMQRadio', queue_size=queue_size)
        Logger.debug(f"ZMQRadio is ready")

    def _transport_write(self, data: object):
        """
        Publishes a message to the ZeroMQ socket with an optional topic.

        Args:
            data (object): The data object to be serialized and sent.                    
        """
        try:            
            self.socket.send(self.serializer.serialize(data), group='*')
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
        Logger.debug(f"ZMQRadio is terminated.")
