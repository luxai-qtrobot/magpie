from abc import ABC, abstractmethod
from luxai.magpie.utils.logger import Logger


class AckTimeoutError(TimeoutError):
    pass

class ReplyTimeoutError(TimeoutError):
    pass


class RpcRequester(ABC):
    """
    RpcRequester is an abstract base class that defines the interface for
    performing RPC-style request/response calls over an abstract transport.

    Without schema — raw dict in/out, current MAGPIE behaviour::

        client = ZMQRpcRequester("tcp://127.0.0.1:5556")
        response = client.call({"method": "add", "a": 1, "b": 2})

    With schema=JsonRpcSchema() — typed calls, JSON-RPC envelope handled
    automatically. Two equivalent call styles::

        client = ZMQRpcRequester("tcp://127.0.0.1:5556", schema=schema)

        # base call
        result = client.call("add", a=1, b=2)
        result = client.call("add", a=1, b=2, _timeout=5.0)

        # proxy — same behaviour, method name from attribute
        result = client.add(a=1, b=2)
        result = client.add(a=1, b=2, _timeout=5.0)
    """

    def __init__(self, name: str = None, schema=None):
        """
        Args:
            name: Display name for logging. Defaults to class name.
            schema: JsonRpcSchema instance. When set, enables typed call() and
                proxy method access via __getattr__.
        """
        self.name = name if name is not None else self.__class__.__name__
        self._closed = False
        self._schema = schema

    @abstractmethod
    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        """
        Performs the actual transport-level call for a single request/response.

        Subclasses should:
          - serialize the request_obj if needed
          - send it through the transport
          - wait for and receive the reply
          - deserialize the reply into a Python object

        Raises:
            ReplyTimeoutError: If no reply arrives in time.
            AckTimeoutError: If no acknowledgment arrives in time.
        """
        pass

    @abstractmethod
    def _transport_close(self) -> None:
        """Closes the underlying transport and releases any associated resources."""
        pass

    def call(self, request_obj: object, timeout: float = None, **params) -> object:
        """
        Perform an RPC call.

        Without schema::

            client.call({"key": "val"}, timeout=5.0)

        With schema — pass method name as first arg, params as kwargs.
        Use ``_timeout`` to set the transport timeout without colliding with
        method parameter names::

            client.call("add", a=3, b=4)
            client.call("add", a=3, b=4, _timeout=5.0)

        Raises:
            RuntimeError: If the requester is already closed.
            JsonRpcError: If the server returns a JSON-RPC error (schema mode).
            ReplyTimeoutError: If no reply arrives in time.
            AckTimeoutError: If no acknowledgment arrives in time.
        """
        if self._closed:
            raise RuntimeError(f"{self.name} is closed")
        try:
            if self._schema is not None and isinstance(request_obj, str):
                _timeout = params.pop("_timeout", timeout)
                envelope = self._schema.wrap(request_obj, params or None)
                response = self._transport_call(envelope, timeout=_timeout)
                return self._schema.unwrap(response)
            return self._transport_call(request_obj, timeout=timeout)
        except Exception as e:
            Logger.warning(f"{self.name}: RPC call failed: {e}")
            raise

    def __getattr__(self, name: str):
        """
        Proxy attribute access to RPC method calls when schema is set.

        ``client.face_look(l_eye=[10,5], r_eye=[-10,5])`` becomes
        ``client.call("face_look", l_eye=[10,5], r_eye=[-10,5])``.

        ``_timeout`` in kwargs is forwarded as the transport timeout.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            schema = object.__getattribute__(self, "_schema")
        except AttributeError:
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")
        if schema is None:
            raise AttributeError(
                f"'{type(self).__name__}' has no attribute '{name}' "
                "(set schema= on the constructor to enable method proxies)"
            )
        def proxy(**kwargs):
            return self.call(name, **kwargs)
        return proxy

    def close(self) -> None:
        """Closes the requester and its underlying transport."""
        self._closed = True
        self._transport_close()
