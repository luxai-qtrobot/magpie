from luxai.magpie.utils.logger import Logger
from luxai.magpie.nodes.process_node import ProcessNode
from luxai.magpie.transport.stream_reader import StreamReader

class SinkNode(ProcessNode):
    """
    SinkNode class.
    
    This class represents a node in a processing pipeline that only reads data 
    from a stream and does not write data to any output stream. It inherits from 
    the ProcessNode class but disables the stream writer functionality by setting 
    it to None.
    """

    def __init__(self, 
                 stream_reader: StreamReader,
                 name=None,                 
                 paused=False,
                 setup_kwargs={}):
        """
        Initializes the SinkNode with a stream reader.
        
        Args:
            stream_reader (StreamReader): An object responsible for reading data from a stream.
            name (str, optional): The name of the node. Defaults to None, in which case the class name is used.
            paused (bool, optional): Whether the node should start in a paused state. Defaults to False.
        
        Raises:
            ValueError: If the stream_reader is None, indicating that a valid StreamReader is required.
        """
        if stream_reader is None:
            raise ValueError("stream_reader cannot be None")
          
        super().__init__(name=name,
                         stream_writer=None,  # Disable stream writer for SinkNode
                         stream_reader=stream_reader, 
                         paused=paused,
                         setup_kwargs=setup_kwargs)
