from abc import ABC, abstractmethod


class BaseSchema(ABC):

    @abstractmethod
    def dispatch(self, request_obj: object) -> object:
        """
        Dispatch a deserialized request and return a response object,
        or None if no reply should be sent (e.g. notifications).
        """
        pass

    @abstractmethod
    def wrap(self, method: str, params: dict = None) -> object:
        """
        Build a request envelope for the given method and params.
        Used by RpcRequester.call() on the requester side.
        """
        pass

    @abstractmethod
    def unwrap(self, response: object) -> object:
        """
        Extract the result from a response envelope.
        Raises an appropriate exception if the response contains an error.
        """
        pass
