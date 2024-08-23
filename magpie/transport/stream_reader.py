from abc import ABC, abstractmethod
from queue import Empty, Queue
from threading import Event, Thread
from magpie.utils.logger import Logger

class StreamReader(ABC):
    """
    StreamReader is an abstract base class that defines the structure for creating a stream reader
    that can read data from a specific transport mechanism in a background thread.

    This class is designed to be subclassed, where the subclass provides specific implementations
    of the methods to read from and close the underlying data transport. The StreamReader class
    handles the queuing of read data and provides thread-safe access to this data if queue_size > 0. 
    """
    def __init__(self, name=None, queue_size=1):
        """
        Initializes the StreamReader with an optional name and sets up the internal queue and thread.

        Args:
            name (str, optional): The name of the stream reader. Defaults to the class name if not provided.
            queue_size (int, optional): The maximum size of the queue for storing read data. Defaults to 1.
        """
        self.name = name if name is not None else self.__class__.__name__
        self.queue_size = queue_size
        if queue_size > 0:
            self.reader_queue = Queue(maxsize=queue_size)
            self.reader_close_event = Event()
            self.thread = Thread(target=self._read_thread)
            self.thread.start()

    @abstractmethod
    def _transport_read_blocking(self) -> object:
        """
        Abstract method to be implemented by subclasses to define how to read data from the underlying transport.

        This method should block until data is available.

        Returns:
            object: The data read from the underlying transport.
        """
        pass

    @abstractmethod
    def _transport_close(self):
        """
        Abstract method to be implemented by subclasses to define how to close the underlying transport.

        This method should perform any necessary cleanup when the stream reader is closed.
        """
        pass

    def _read_thread(self):
        """
        Internal method run in a separate thread to continuously read data from the transport
        and store it in the queue until the stream is closed.

        This method checks if data is available, reads it using the _transport_read_blocking method,
        and puts it into the queue. If the queue is full, the oldest data is removed to make room
        for new data.
        """
        while not self.reader_close_event.is_set():
            try:
                data = self._transport_read_blocking()
                if not data:
                    continue
                if self.reader_queue.full():
                    self.reader_queue.get_nowait()  # Remove the oldest item if the queue is full
                self.reader_queue.put(data)
            except Exception as e:
                Logger.warning(f"{self.name} _read_thread: {str(e)}")

    def read(self) -> object:
        """
        Reads data from the stream in a blocking manner.

        If a queue is used (queue_size > 0), this method retrieves data from the queue. 
        If no queue is used, it directly calls the _transport_read_blocking method.

        Returns:
            object: The data read from the stream.
        """
        if self.queue_size <= 0:
            return self._transport_read_blocking()
        
        while not self.reader_close_event.is_set():
            try:
                return self.reader_queue.get(timeout=2)
            except Empty:
                pass

    def close(self):
        """
        Closes the stream reader and performs any necessary cleanup.

        If a queue is used, this method stops the reading thread and waits for it to finish.
        It also calls the _transport_close method to close the underlying transport.
        """
        if self.queue_size > 0:
            self.reader_close_event.set()  # Signal the thread to stop
            self._transport_close()  # Close the transport
            self.thread.join()  # Wait for the reading thread to finish
        else:
            self._transport_close()  # Close the transport
