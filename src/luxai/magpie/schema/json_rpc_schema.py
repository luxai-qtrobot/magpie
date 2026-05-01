import inspect
import itertools
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
    """Derive JSON Schema input object from a function's type hints."""
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


def _params_list_to_input_schema(params: list) -> dict:
    """Convert [(name, type), ...] to a JSON Schema object. All params are required."""
    properties = {}
    required = []
    for name, py_type in params:
        properties[name] = _python_type_to_json_schema(py_type)
        required.append(name)
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _idl_params_to_input_schema(params: dict) -> dict:
    """
    Convert the MAGPIE custom IDL params dict to a JSON Schema object.

    IDL format per param::

        "l_eye": {"type": "array", "items": "integer", "required": true}
        "duration": {"type": "number", "default": 0}
    """
    properties = {}
    required = []
    for name, spec in params.items():
        prop = {}
        if "type" in spec:
            prop["type"] = spec["type"]
        items = spec.get("items")
        if items is not None:
            prop["items"] = {"type": items} if isinstance(items, str) else items
        if "default" in spec:
            prop["default"] = spec["default"]
        properties[name] = prop
        if spec.get("required", False):
            required.append(name)
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


class JsonRpcSchema(BaseSchema):
    """
    JSON-RPC 2.0 schema — dispatch layer for the responder side,
    envelope builder/unwrapper for the requester side.

    Methods can be defined in four ways:

    A) From a dict/file (custom IDL)::

        schema = JsonRpcSchema.from_dict(ROBOT_API)
        schema = JsonRpcSchema.from_json_file("robot_api.json")
        schema = JsonRpcSchema.from_json_string('{"add": {...}}')

    B) Programmatic, without handler (requester side)::

        schema.register("face_look", params=[("l_eye", list), ("r_eye", list)])

    C) Decorator — defines shape and attaches handler together (responder side)::

        @schema.method()
        def add(a: int, b: int) -> int:
            return a + b

    D) Separate definition and handler (responder side, after from_dict)::

        @schema.handler("face_look")
        def handle_face_look(l_eye, r_eye, duration=0.0):
            ...
    """

    def __init__(self):
        self._methods: dict = {}  # name → {"func", "description", "input_schema"}
        self._id_counter = itertools.count(1)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        func: Callable = None,
        description: str = None,
        params: list = None,
        input_schema: dict = None,
    ) -> None:
        """
        Register a method by name.

        Args:
            name: Method name.
            func: Handler callable. Optional — omit on the requester side or
                when attaching the handler later via @schema.handler().
            description: Human-readable description. Inferred from func docstring
                if not provided.
            params: [(name, type), ...] shorthand for defining input schema without
                a handler function. All listed params are treated as required.
            input_schema: Explicit JSON Schema object for the params. Takes
                precedence over params and func type hints.
        """
        if input_schema is None:
            if params is not None:
                input_schema = _params_list_to_input_schema(params)
            elif func is not None:
                input_schema = _infer_input_schema(func)
            else:
                input_schema = {"type": "object"}

        if description is None and func is not None:
            description = inspect.getdoc(func) or ""

        self._methods[name] = {
            "func": func,
            "description": description or "",
            "input_schema": input_schema,
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

        Use after loading a schema from dict/file to attach handlers::

            schema = JsonRpcSchema.from_dict(ROBOT_API)

            @schema.handler("face_look")
            def handle_face_look(l_eye, r_eye, duration=0.0):
                ...
        """
        def decorator(func: Callable) -> Callable:
            if name not in self._methods:
                raise KeyError(
                    f"'{name}' is not defined in this schema. "
                    "Call register() or from_dict() first."
                )
            self._methods[name]["func"] = func
            if not self._methods[name]["description"]:
                self._methods[name]["description"] = inspect.getdoc(func) or ""
            return func
        return decorator

    # ------------------------------------------------------------------
    # Class constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: dict) -> "JsonRpcSchema":
        """
        Load a schema from the MAGPIE custom IDL dict format::

            {
                "face_look": {
                    "description": "Move robot eyes",
                    "params": {
                        "l_eye":    {"type": "array", "required": true},
                        "r_eye":    {"type": "array", "required": true},
                        "duration": {"type": "number", "default": 0}
                    },
                    "returns": {"type": "boolean"}
                }
            }

        Methods are defined without handlers. Attach handlers with
        @schema.handler() on the responder side.
        """
        schema = cls()
        for method_name, spec in data.items():
            schema.register(
                method_name,
                func=None,
                description=spec.get("description", ""),
                input_schema=_idl_params_to_input_schema(spec.get("params", {})),
            )
        return schema

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
