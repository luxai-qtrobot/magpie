import inspect
import itertools
import json
from typing import Callable, Optional, get_type_hints

from luxai.magpie.utils.logger import Logger
from .base_schema import BaseSchema


# Standard JSON-RPC 2.0 error codes
PARSE_ERROR      = -32700
INVALID_REQUEST  = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS   = -32602
INTERNAL_ERROR   = -32603


class JsonRpcError(Exception):
    """Raised by JsonRpcSchema.unwrap() when the server returns a JSON-RPC error."""
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

    def __repr__(self):
        return f"JsonRpcError(code={self.code}, message={self.message!r})"


# ------------------------------------------------------------------
# Internal schema helpers
# ------------------------------------------------------------------

def _python_type_to_json_schema(py_type) -> dict:
    mapping = {
        str:   {"type": "string"},
        int:   {"type": "integer"},
        float: {"type": "number"},
        bool:  {"type": "boolean"},
        dict:  {"type": "object"},
        list:  {"type": "array"},
    }
    return mapping.get(py_type, {"type": "string"})


def _infer_input_schema(func: Callable) -> dict:
    """Derive a JSON Schema input object from a function's type hints."""
    try:
        hints = get_type_hints(func)
        sig = inspect.signature(func)
        properties = {}
        required = []
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls") or param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            py_type = hints.get(param_name)
            properties[param_name] = _python_type_to_json_schema(py_type) if py_type else {}
            if param.default is inspect.Parameter.empty:
                required.append(param_name)
        schema = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        return schema
    except Exception:
        return {"type": "object"}


def _infer_output_schema(func: Callable) -> Optional[dict]:
    """Derive a JSON Schema from the function's return type annotation."""
    try:
        hints = get_type_hints(func)
        return_type = hints.get("return")
        if return_type is None or return_type is type(None):
            return None
        return _python_type_to_json_schema(return_type)
    except Exception:
        return None


class JsonRpcSchema(BaseSchema):
    """
    JSON-RPC 2.0 schema — dispatch layer for the responder side,
    envelope builder/unwrapper for the requester side.

    Methods can be defined in three ways:

    A) Decorator — defines shape and attaches handler together::

        @schema.method()
        def add(a: float, b: float) -> float:
            return a + b

    B) Programmatic register with explicit JSON Schema::

        schema.register(
            name="add",
            description="Add two numbers",
            input_schema={
                "type": "object",
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"],
            },
            output_schema={"type": "number"},
        )

    C) Load from a JSON file, then attach handlers::

        schema = JsonRpcSchema.from_json_file("api.json")

        @schema.handler("face_look")
        def handle_face_look(l_eye, r_eye, duration=0.0):
            ...
    """

    def __init__(self):
        self._methods: dict = {}  # name → {"func", "description", "input_schema", "output_schema"}
        self._id_counter = itertools.count(1)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        func: Callable = None,
        description: str = None,
        input_schema: dict = None,
        output_schema: dict = None,
    ) -> None:
        """
        Register a method by name.

        Args:
            name: Method name.
            func: Handler callable. Optional — omit on the requester side or
                when attaching the handler later via @schema.handler().
            description: Human-readable description. Inferred from func docstring
                if not provided.
            input_schema: JSON Schema object describing the parameters. Inferred
                from func type hints if not provided.
            output_schema: JSON Schema object describing the return value. Optional.
                Inferred from func return type annotation if not provided.
        """
        if input_schema is None:
            input_schema = _infer_input_schema(func) if func is not None else {"type": "object"}

        if output_schema is None and func is not None:
            output_schema = _infer_output_schema(func)

        if description is None and func is not None:
            description = inspect.getdoc(func) or ""

        self._methods[name] = {
            "func": func,
            "description": description or "",
            "input_schema": input_schema,
            "output_schema": output_schema,
        }

    def method(self, name: str = None):
        """
        Decorator — registers a method using the function name (or explicit name)
        and infers its input schema from type hints.

        Use on the responder side when definition and implementation are together::

            @schema.method()
            def add(a: int, b: int) -> int:
                return a + b

        Use on the requester side with a stub body to define shape only::

            @schema.method()
            def face_look(l_eye: list, r_eye: list, duration: float = 0.0): ...
        """
        def decorator(func: Callable) -> Callable:
            self.register(name or func.__name__, func)
            return func
        return decorator

    def handler(self, name: str):
        """
        Decorator — attaches an implementation to an already-defined method.

        Use after loading a schema from JSON or after register()::

            schema = JsonRpcSchema.from_json_file("api.json")

            @schema.handler("face_look")
            def handle_face_look(l_eye, r_eye, duration=0.0):
                ...
        """
        def decorator(func: Callable) -> Callable:
            if name not in self._methods:
                raise KeyError(
                    f"'{name}' is not defined in this schema. "
                    "Call register() or from_json_* first."
                )
            self._methods[name]["func"] = func
            if not self._methods[name]["description"]:
                self._methods[name]["description"] = inspect.getdoc(func) or ""
            return func
        return decorator

    # ------------------------------------------------------------------
    # Load from JSON / YAML
    # ------------------------------------------------------------------

    @classmethod
    def from_json(cls, data: list, **kwargs) -> "JsonRpcSchema":
        """
        Load schema from a parsed Python list of method objects::

            schema = JsonRpcSchema.from_json([
                {
                    "name": "add",
                    "description": "Add two numbers",
                    "inputSchema": {"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}}, "required": ["a", "b"]},
                },
            ])

        Methods are registered without handlers. Attach handlers with
        ``@schema.handler(name)`` on the responder side.
        """
        if not isinstance(data, list):
            raise ValueError("Expected a list of method objects")

        schema = cls(**kwargs)
        for entry in data:
            method_name = entry.get("name")
            if not method_name:
                raise ValueError(f"Method entry missing 'name' field: {entry}")
            schema.register(
                name=method_name,
                func=None,
                description=entry.get("description", ""),
                input_schema=entry.get("inputSchema") or {"type": "object"},
                output_schema=entry.get("outputSchema"),
            )
        return schema

    @classmethod
    def from_json_string(cls, s: str, **kwargs) -> "JsonRpcSchema":
        """Load schema from a JSON string."""
        return cls.from_json(json.loads(s), **kwargs)

    @classmethod
    def from_json_file(cls, path: str, **kwargs) -> "JsonRpcSchema":
        """Load schema from a JSON file."""
        with open(path) as f:
            return cls.from_json(json.load(f), **kwargs)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _error(self, req_id, code: int, message: str) -> dict:
        return {"jsonrpc": "2.0", "error": {"code": code, "message": message}, "id": req_id}

    def _result(self, req_id, result: object) -> dict:
        return {"jsonrpc": "2.0", "result": result, "id": req_id}

    # ------------------------------------------------------------------
    # Client-side helpers
    # ------------------------------------------------------------------

    def wrap(self, method: str, params: dict = None) -> dict:
        """Build a JSON-RPC 2.0 request envelope for the given method and params."""
        req = {"jsonrpc": "2.0", "method": method, "id": next(self._id_counter)}
        if params:
            req["params"] = params
        return req

    def unwrap(self, response: object) -> object:
        """
        Extract the result from a JSON-RPC 2.0 response dict.

        Raises:
            JsonRpcError: if the response contains an error.
        """
        if not isinstance(response, dict):
            raise JsonRpcError(INVALID_REQUEST, f"Invalid response: {response!r}")
        if "error" in response:
            err = response["error"]
            raise JsonRpcError(err.get("code", INTERNAL_ERROR), err.get("message", "Unknown error"))
        return response.get("result")

    # ------------------------------------------------------------------
    # Dispatch (responder side)
    # ------------------------------------------------------------------

    def _dispatch_single(self, req: dict) -> Optional[dict]:
        if not isinstance(req, dict):
            return self._error(None, INVALID_REQUEST, "Request must be an object")

        req_id = req.get("id")
        method_name = req.get("method")
        params = req.get("params")

        if not method_name:
            return self._error(req_id, INVALID_REQUEST, "Missing 'method' field")

        entry = self._methods.get(method_name)
        if entry is None:
            if req_id is None:
                return None  # unknown notification — silently ignore
            return self._error(req_id, METHOD_NOT_FOUND, f"Method not found: {method_name}")

        func = entry["func"]
        if func is None:
            if req_id is None:
                return None
            return self._error(req_id, METHOD_NOT_FOUND, f"Method not implemented: {method_name}")

        try:
            if params is None:
                result = func()
            elif isinstance(params, dict):
                result = func(**params)
            elif isinstance(params, list):
                result = func(*params)
            else:
                return self._error(req_id, INVALID_PARAMS, "'params' must be object or array")
        except TypeError as e:
            return self._error(req_id, INVALID_PARAMS, str(e))
        except Exception as e:
            Logger.warning(f"JsonRpcSchema: handler '{method_name}' raised: {e}")
            return self._error(req_id, INTERNAL_ERROR, str(e))

        if req_id is None:
            return None  # notification — no reply

        return self._result(req_id, result)

    def dispatch(self, request_obj: object) -> object:
        if isinstance(request_obj, list):
            responses = [
                r for r in (self._dispatch_single(req) for req in request_obj)
                if r is not None
            ]
            return responses or None
        return self._dispatch_single(request_obj)
