from abc import ABC, abstractmethod
from typing import Callable
from threading import Event
from magpie.utils.logger import Logger


RpcHandlerType = Callable[[object], object]

class RpcResponder(ABC):
    """
    RpcResponder is an abstract base class that defines the interface for
    handling RPC-style request/response calls over an abstract transport.

    Subclasses implement the underlying transport logic (e.g., ZeroMQ ROUTER).
    """

    def __init__(self, name: str = None):
        """
        Initializes the RpcResponder.

        Args:
            name (str, optional): The name for this responder. Defaults to class name.
        """
        self.name = name if name is not None else self.__class__.__name__
        self._closed = Event()

    @abstractmethod
    def _transport_recv(self, timeout: float = None) -> tuple:
        """
        Receives a request from the transport layer.

        Implementations should:
          - block until a request arrives (or timeout is reached)
          - deserialize the request into a Python object
          - return (request_obj, client_ctx), where client_ctx is an
            opaque context needed to send the reply (e.g. ZMQ identity).

        Args:
            timeout (float, optional): Maximum time to wait in seconds.

        Returns:
            tuple: (request_obj, client_ctx)

        Raises:
            TimeoutError: If no request is received within the timeout.
            Exception: For transport-level failures.
        """
        pass

    @abstractmethod
    def _transport_send(self, response_obj: object, client_ctx: object) -> None:
        """
        Sends a response back to the client via the transport layer.

        Implementations should:
          - serialize the response_obj if needed
          - send it using client_ctx from _transport_recv()

        Args:
            response_obj (object): The response payload.
            client_ctx (object): Transport-specific context (e.g. identity).
        """
        pass

    @abstractmethod
    def _transport_close(self) -> None:
        """
        Closes the underlying transport and releases any associated resources.
        """
        pass

    
    def handle_once(self, handler: RpcHandlerType, timeout: float = None) -> bool:
        """
        Handles a single incoming request using the given handler.

        Args:
            handler (callable): Function with signature handler(request_obj) -> response_obj.
            timeout (float, optional): Timeout in seconds for waiting for a request.

        Returns:
            bool: True if a request was handled, False otherwise (e.g., timeout).

        Raises:
            RuntimeError: If the responder is already closed.
            TimeoutError: If no request arrives in time.
            Exception: For transport-level or handler errors.
        """
        if self._closed.is_set():
            raise RuntimeError(f"{self.name} is closed")

        try:
            request_obj, client_ctx = self._transport_recv(timeout=timeout)
        except TimeoutError:
            # no request within timeout
            return False

        # Let the user-defined handler process the request
        response_obj = handler(request_obj)

        # Send the response
        self._transport_send(response_obj, client_ctx)
        return True

    def close(self) -> None:
        """
        Closes the responder and its underlying transport.
        """
        if not self._closed.is_set():
            self._closed.set()
            try:
                self._transport_close()
            except Exception as e:
                Logger.warning(f"{self.name}: error during close: {e}")
            Logger.debug(f"{self.name} closed.")

    def closed(self) -> bool:
        """
        Indicates whether this responder has been closed.

        Returns:
            bool: True if closed, False otherwise.
        """
        return self._closed.is_set()

    def __del__(self):
        """
        Ensures cleanup on deletion.
        """
        try:
            self.close()
        except Exception:
            pass
