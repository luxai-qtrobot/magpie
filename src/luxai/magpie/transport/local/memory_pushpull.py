import time
from queue import Queue, Empty
from threading import Event
from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.transport.stream_reader import StreamReader
from luxai.magpie.utils.logger import Logger

class MemmoryPushPull(StreamWriter, StreamReader):
    """
    MemmoryStreamer class.
    """

    def __init__(self, maxsize=0):
        StreamWriter.__init__(self, name='MemmoryStreamer', queue_size=0)
        StreamReader.__init__(self, queue_size=0)
        self.queue = Queue(maxsize=maxsize)
        self.close_event = Event()


    def _transport_write(self, data: object, topic:str):
        try:
            self.queue.put_nowait(data)
        except Exception as e:
            Logger.warning(f"{self.name} write failed with: {str(e)}")
            raise IOError

    def _transport_read_blocking(self) -> (object, str):
        while not self.close_event.is_set():
            try:
                return self.queue.get(timeout=2), ""
            except Empty:
                pass            
        time.sleep(0.5)  # this helps to avoid having infinit loop in calling thread when the MemmoryStreamer is closed.
        return None
        
    def _transport_close(self):
        self.close_event.set()

    def __del__(self):
        self._transport_close() 

    def close(self):
        StreamReader.close(self)
        StreamWriter.close(self)