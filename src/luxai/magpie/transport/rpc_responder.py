from abc import ABC, abstractmethod
from typing import Callable, Optional
from luxai.magpie.utils.logger import Logger


RpcHandlerType = Callable[[object], object]

class RpcResponder(ABC):
    """
    RpcResponder is an abstract base class that defines the interface for
    handling RPC-style request/response calls over an abstract transport.

    Subclasses implement the underlying transport logic (e.g., ZeroMQ ROUTER).
    """

    def __init__(self, name: str = None, schema=None):
        """
        Initializes the RpcResponder.

        Args:
            name (str, optional): The name for this responder. Defaults to class name.
            schema (BaseSchema, optional): Schema for automatic request dispatch.
                When set, handler is not required in handle_once().
        """
        self.name = name if name is not None else self.__class__.__name__
        self._closed = False
        self._schema = schema

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

    
    def handle_once(self, handler: Optional[RpcHandlerType] = None, timeout: float = None) -> bool:
        """
        Handles a single incoming request.

        When a schema is set on this responder, handler is optional — the schema
        dispatches the request automatically. When no schema is set, handler is
        required.

        Args:
            handler (callable, optional): Function with signature handler(request_obj) -> response_obj.
                Not needed when schema= was passed to the constructor.
            timeout (float, optional): Timeout in seconds for waiting for a request.

        Returns:
            bool: True if a request was handled, False on timeout.

        Raises:
            RuntimeError: If the responder is already closed.
            ValueError: If neither handler nor schema is available.
            Exception: For transport-level or handler errors.
        """
        if self._closed:
            raise RuntimeError(f"{self.name} is closed")
        if handler is None and self._schema is None:
            raise ValueError(f"{self.name}: handler is required when no schema is set")

        try:
            request_obj, client_ctx = self._transport_recv(timeout=timeout)
        except TimeoutError:
            return False

        if self._schema is not None:
            response_obj = self._schema.dispatch(request_obj)
            if response_obj is not None:
                self._transport_send(response_obj, client_ctx)
        else:
            response_obj = handler(request_obj)
            self._transport_send(response_obj, client_ctx)

        return True

    def close(self) -> None:
        """
        Closes the responder and its underlying transport.
        """
        self._closed = True
        self._transport_close()
