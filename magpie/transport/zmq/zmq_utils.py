import sys
from threading import Lock
from magpie.utils.logger import Logger

try:
    import zmq
except ImportError as e:
    Logger.error(f"Could not import zmq. Please install it using 'pip install pyzmq'.")
    sys.exit()


class ZMQContext:
    """
    ZMQContext class.
    
    This class implements the Singleton pattern to manage a single ZeroMQ context across the application.
    """
    
    _instance = None  # The singleton instance of zmq.Context
    _lock = Lock()  # A lock to ensure thread-safety when creating the singleton instance

    @classmethod
    def get_instance(cls):
        """
        Returns the singleton instance of the ZeroMQ context.
        
        This method implements double-checked locking to ensure that the context is created 
        only once, even in multi-threaded environments. If the instance does not exist, it is 
        created inside a thread-safe block to avoid race conditions.

        Returns:
            zmq.Context: The singleton instance of the ZeroMQ context.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-checked locking
                    cls._instance = zmq.Context()
        return cls._instance
