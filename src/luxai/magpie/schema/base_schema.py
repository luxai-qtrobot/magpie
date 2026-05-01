import json
from abc import ABC, abstractmethod


class BaseSchema(ABC):

    @abstractmethod
    def dispatch(self, request_obj: object) -> object:
        """
        Dispatch a deserialized request and return a response object,
        or None if no reply should be sent (e.g. notifications).
        """
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> "BaseSchema":
        """
        Load schema definition from a dict.
        Format is subclass-specific.
        """
        pass

    @classmethod
    def from_json_string(cls, s: str) -> "BaseSchema":
        """Load schema definition from a JSON string."""
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_json_file(cls, path: str) -> "BaseSchema":
        """Load schema definition from a JSON file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))
