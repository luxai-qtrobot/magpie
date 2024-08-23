from magpie.utils.logger import Logger
from magpie.nodes.base_node import BaseNode
from magpie.transport.stream_reader import StreamReader
from magpie.transport.stream_writer import StreamWriter

class ProcessNode(BaseNode):
    """
    ProcessNode class.
    
    This class represents a node in a processing pipeline that reads data from a
    stream and writes data to another stream. It inherits from the BaseNode class
    and adds functionality to handle stream input and output via StreamReader
    and StreamWriter objects.
    """

    def __init__(self, 
                 stream_reader: StreamReader,
                 stream_writer: StreamWriter,
                 name=None,
                 paused=False,
                 setup_kwargs={}):
        """
        Initializes the ProcessNode with a stream reader and a stream writer.
        
        Args:
            stream_reader (StreamReader): An object responsible for reading data from a stream.
            stream_writer (StreamWriter): An object responsible for writing data to a stream.
            name (str, optional): The name of the node. Defaults to None, in which case the class name is used.
            paused (bool, optional): Whether the node should start in a paused state. Defaults to False.
        """
        super().__init__(name=name, paused=paused, setup_kwargs=setup_kwargs)
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer

    def terminate(self, timeout=None):
        """
        Terminates the ProcessNode by closing its stream reader and writer, 
        and then calling the terminate method of the BaseNode class.
        
        Args:
            timeout (float, optional): The maximum time to wait for the node to terminate. Defaults to None.
        """
        if self.stream_writer:
            self.stream_writer.close()
        if self.stream_reader:
            self.stream_reader.close()
        super().terminate(timeout)
