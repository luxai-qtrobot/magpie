from typing import Tuple
from abc import ABC, abstractmethod
from queue import Empty, Queue
from threading import Event, Thread
import time

from luxai.magpie.utils.logger import Logger


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

        # Internal flag to make close() idempotent.
        self._closed = False

        if queue_size > 0:
            self.reader_queue = Queue(maxsize=queue_size)
            self.reader_close_event = Event()
            # Daemon thread so a forgotten close() won't block interpreter shutdown.
            self.thread = Thread(target=self._read_thread, name=self.name, daemon=True)
            self.thread.start()

    @abstractmethod
    def _transport_read_blocking(self, timeout: float = None) -> Tuple[object, str] | None:
        """
        Abstract method to be implemented by subclasses to define how to read data from the underlying transport.

        This method should block until data is available.

        Args:
            timeout (float, optional): Maximum time to wait for data in seconds.
        Returns:
            object: The data read from the underlying transport.            

        Raises:
            TimeoutError: If no data read within the timeout.
            Exception: For transport-level failures.            
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
        # Use a bounded timeout so we periodically check reader_close_event and can exit promptly.
        poll_timeout = 1.0

        while not self.reader_close_event.is_set():
            try:
                raw_data = self._transport_read_blocking(timeout=poll_timeout)
                if raw_data is None:
                    # Transport returned no data but not an error; try again.
                    continue

                data, topic = raw_data
                if self.reader_queue.full():
                    # Logger.debug(f"{self.name} queue is full. dropping old message.")
                    try:
                        self.reader_queue.get_nowait()  # Remove the oldest item if the queue is full
                    except Empty:
                        # Race condition: queue became empty; just continue.
                        pass

                self.reader_queue.put((data, topic))
            except TimeoutError:
                # Normal timeout, loop back and check close event.
                continue
            except Exception as e:
                Logger.warning(f"{self.name} _read_thread: {str(e)}")
                # Optionally break on fatal errors; for now, continue trying.
                continue

    def read(self, timeout: float = None) -> Tuple[object, str] | None:
        """
        Reads data from the stream in a blocking manner.

        If a queue is used (queue_size > 0), this method retrieves data from the queue. 
        If no queue is used, it directly calls the _transport_read_blocking method.

        Args:
            timeout (float, optional): Maximum time to wait for data in seconds.
        Returns:
            object: The data read from the underlying transport.            

        Raises:
            TimeoutError: If no data read within the timeout.
            Exception: For transport-level failures.
        """
        # Direct transport read if no queue is used.
        if self.queue_size <= 0:
            return self._transport_read_blocking(timeout=timeout)

        # If reader is already closed, fail fast.
        if getattr(self, "reader_close_event", None) is not None and self.reader_close_event.is_set():
            #raise RuntimeError(f"{self.name}: reader is closed")
            return None

        # Poll the queue in chunks of up to 1 second so we can check timeouts and close events.
        if timeout is None:
            queue_timeout = 1.0
        else:
            queue_timeout = min(timeout, 1.0)

        start_t = time.time()
        while not self.reader_close_event.is_set():
            try:
                return self.reader_queue.get(timeout=queue_timeout)
            except Empty:
                # No data yet, check timeout.
                pass

            # Check if timeout occurred
            if timeout is not None and (time.time() - start_t) > timeout:
                raise TimeoutError(f"{self.name}: no data received within {timeout} seconds")

        # Reader was closed while waiting.
        # raise RuntimeError(f"{self.name}: reader closed while waiting for data")

    def close(self):
        """
        Closes the stream reader and performs any necessary cleanup.

        If a queue is used, this method stops the reading thread and waits for it to finish.
        It also calls the _transport_close method to close the underlying transport.
        """
        # Make close() safe to call multiple times.
        if self._closed:
            return

        self._closed = True

        if self.queue_size > 0:
            # Signal the thread to stop.
            self.reader_close_event.set()
            # Wait for the reading thread to finish (bounded wait).
            try:
                self.thread.join(timeout=1.0)
            except RuntimeError:
                # Thread may never have been started or already finished.
                pass

            # Close the transport after the read loop has stopped.
            try:
                self._transport_close()
            except Exception as e:
                Logger.warning(f"{self.name} close: error closing transport: {e}")
        else:
            # Close the transport immediately if no queue is used.
            try:
                self._transport_close()
            except Exception as e:
                Logger.warning(f"{self.name} close: error closing transport: {e}")
