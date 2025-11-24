from magpie.utils.logger import Logger
from magpie.nodes.process_node import ProcessNode
from magpie.transport.stream_writer import StreamWriter

class SourceNode(ProcessNode):
    """
    SourceNode class.
    
    This class represents a node in a processing pipeline that only writes data 
    to a stream and does not read data from any input stream. It inherits from 
    the ProcessNode class but disables the stream reader functionality by setting 
    it to None.
    """

    def __init__(self, 
                 stream_writer: StreamWriter,
                 name=None,                 
                 paused=False,
                 setup_kwargs={}):
        """
        Initializes the SourceNode with a stream writer.
        
        Args:
            stream_writer (StreamWriter): An object responsible for writing data to a stream.
            name (str, optional): The name of the node. Defaults to None, in which case the class name is used.
            paused (bool, optional): Whether the node should start in a paused state. Defaults to False.
        
        Raises:
            ValueError: If the stream_writer is None, indicating that a valid StreamWriter is required.
        """
        if stream_writer is None:
            raise ValueError("stream_writer cannot be None")
          
        super().__init__(name=name,
                         stream_writer=stream_writer,  # Set stream writer for SourceNode
                         stream_reader=None,  # Disable stream reader for SourceNode
                         paused=paused,
                         setup_kwargs=setup_kwargs)
