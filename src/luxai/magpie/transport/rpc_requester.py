from abc import ABC, abstractmethod
from luxai.magpie.utils.logger import Logger

# lets define two subclass of TimeoutError: AckTimeoutError and ReplyTimeoutError
class AckTimeoutError(TimeoutError):
    pass

class ReplyTimeoutError(TimeoutError):
    pass


class RpcRequester(ABC):
    """
    RpcRequester is an abstract base class that defines the interface for
    performing RPC-style request/response calls over an abstract transport.

    Subclasses are responsible for handling any (de)serialization and
    transport-specific details.
    """

    def __init__(self, name: str = None):
        """
        Initializes the RpcRequester.

        Args:
            name (str, optional): The name for this requester. Defaults to class name.
        """
        self.name = name if name is not None else self.__class__.__name__      
        self._closed = False  

    @abstractmethod
    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        """
        Performs the actual transport-level call for a single request/response.

        Subclasses should:
          - serialize the request_obj if needed
          - send it through the transport
          - wait for and receive the reply
          - deserialize the reply into a Python object

        Args:
            request_obj (object): The request payload.
            timeout (float, optional): Maximum time to wait for a reply in seconds.

        Returns:
            object: The response object.
        
        Raises:
            RuntimeError: If the requester is already closed.
            ReplyTimeoutError: If no reply arrives in time.
            AckTimeoutError: If no acknowledgment for receipt arrives in time.
            Exception: For transport-level errors.
        """
        pass

    @abstractmethod
    def _transport_close(self) -> None:
        """
        Closes the underlying transport and releases any associated resources.
        """
        pass

    def call(self, request_obj: object, timeout: float = None) -> object:
        """
        Performs an RPC call using the underlying transport.

        Args:
            request_obj (object): Request payload to send.
            timeout (float, optional): Timeout in seconds for waiting for a reply.

        Returns:
            object: The response object.

        Raises:
            RuntimeError: If the requester is already closed.
            ReplyTimeoutError: If no reply arrives in time.
            AckTimeoutError: If no acknowledgment for receipt arrives in time.
            Exception: For transport-level errors.
        """
        if self._closed:
            raise RuntimeError(f"{self.name} is closed")
        try:
            return self._transport_call(request_obj, timeout=timeout)
        except Exception as e:
            Logger.warning(f"{self.name}: RPC call failed: {e}")
            raise

    def close(self) -> None:
        """
        Closes the requester and its underlying transport.
        """
        self._closed = True    
        self._transport_close()
