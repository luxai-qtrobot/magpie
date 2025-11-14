from abc import ABC, abstractmethod
from queue import Empty, Queue
from threading import Event, Thread, Lock

from magpie.utils.logger import Logger

class StreamWriter(ABC):
    """
    StreamWriter is an abstract base class that defines the structure for creating a stream writer
    that can write data to a specific transport mechanism in a background thread.

    This class is intended to be subclassed, where the subclass provides specific implementations
    of the methods to write to and close the underlying data transport. The StreamWriter class
    manages the queuing of data to be written if queue_size > 0.
    """

    def __init__(self, name=None, queue_size=1):
        """
        Initializes the StreamWriter with an optional name and sets up the internal queue and thread.

        Args:
            name (str, optional): The name of the stream writer. Defaults to the class name if not provided.
            queue_size (int, optional): The maximum size of the queue for storing data to be written. Defaults to 1.
        """
        self.name = name if name is not None else self.__class__.__name__
        self.queue_size = queue_size

        if queue_size > 0:
            self.writer_queue = Queue(maxsize=queue_size)
            self.writer_close_event = Event()
            self.lock = Lock()
            self.thread = Thread(target=self._write_thread)
            self.thread.start()

    @abstractmethod
    def _transport_write(self, data: object, topic:str):
        """
        Abstract method to be implemented by subclasses to define how to write data to the transport.

        This method should handle the actual writing of the provided data to the underlying transport.

        Args:
            data (object): The data to be written to the transport.
        """
        pass

    @abstractmethod
    def _transport_close(self):
        """
        Abstract method to be implemented by subclasses to define how to close the underlying transport.

        This method should perform any necessary cleanup when the stream writer is closed.
        """
        pass

    def _write_thread(self):
        """
        Internal method that runs in a separate thread to continuously write data from the queue
        to the transport until the stream is closed.

        This method retrieves data from the queue and calls the _transport_write method to
        write it to the transport. If the queue is empty, it waits for new data to arrive.
        """
        while not self.writer_close_event.is_set():
            try:
                # Get data from the queue, waiting up to 2 seconds if the queue is empty.
                data, topic = self.writer_queue.get(timeout=2)
                # Write the retrieved data to the transport.
                self._transport_write(data, topic)
            except Empty:
                # Continue the loop if the queue is empty, waiting for new data.
                pass

    def write(self, data: object, topic:str = None):
        """
        Queues data for writing or writes it directly to the transport, depending on the queue size.

        If the queue size is greater than 0, this method adds the data to the queue.
        If the queue is full, it removes the oldest item before adding the new data.
        If the queue size is 0, it writes the data directly to the transport.

        Args:
            data (object): The data to be written to the transport.
        """
        if self.queue_size <= 0:
            # Write data directly if no queue is used.
            return self._transport_write(data, topic)

        try:
            with self.lock:
                # Check if the queue is full.
                if self.writer_queue.full():
                    # Logger.debug(f"{self.name} queue is full. dropping old message.")
                    # Remove the oldest item to make room for the new data.
                    self.writer_queue.get_nowait()
                # Add the new data to the queue.
                self.writer_queue.put((data, topic))
        except Exception as e:
            # Log any exceptions that occur during the write process.
            Logger.warning(f"{self.name} write: {str(e)}")

    def close(self):
        """
        Closes the stream writer and performs any necessary cleanup.

        This method stops the writing thread and waits for it to finish.
        It also calls the _transport_close method to close the underlying transport.

        If the queue size is greater than 0, it ensures that all queued data is written
        before shutting down.
        """
        if self.queue_size > 0:
            # Signal the writing thread to stop.
            self.writer_close_event.set()
            # Close the underlying transport.
            self._transport_close()
            # Wait for the writing thread to finish.
            self.thread.join()
        else:
            # Close the transport immediately if no queue is used.
            self._transport_close()
