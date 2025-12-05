# tests/fake_stream.py

from queue import Queue, Empty
from luxai.magpie.transport import StreamReader
from luxai.magpie.transport import StreamWriter


class FakeSharedBuffer:
    """A simple shared FIFO queue acting as our transport."""
    def __init__(self):
        self.queue = Queue()
        self.closed = False

    def push(self, data, topic):
        self.queue.put((data, topic))

    def pop(self, timeout=None):
        try:
            return self.queue.get(timeout=timeout)
        except Empty:
            return None

    def close(self):
        self.closed = True


class FakeStreamWriter(StreamWriter):
    """
    Writes into a shared buffer (transport).
    """
    def __init__(self, transport: FakeSharedBuffer, queue_size=0):
        self.transport = transport
        super().__init__(queue_size=queue_size)

    def _transport_write(self, data, topic):
        self.transport.push(data, topic)

    def _transport_close(self):
        self.transport.close()


class FakeStreamReader(StreamReader):
    """
    Reads from a shared buffer (transport).
    """
    def __init__(self, transport: FakeSharedBuffer, queue_size=0):
        self.transport = transport
        super().__init__(queue_size=queue_size)

    def _transport_read_blocking(self, timeout: float = None):
        return self.transport.pop(timeout=2)

    def _transport_close(self):
        self.transport.close()
