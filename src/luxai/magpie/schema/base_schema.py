from abc import ABC, abstractmethod


class BaseSchema(ABC):

    @abstractmethod
    def dispatch(self, request_obj: object) -> object:
        """
        Dispatch a deserialized request and return a response object,
        or None if no reply should be sent (e.g. notifications).
        """
        pass
