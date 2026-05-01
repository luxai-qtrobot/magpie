import json
from typing import Callable

from .json_rpc_schema import JsonRpcSchema, _infer_input_schema


MCP_PROTOCOL_VERSION = "2024-11-05"


def _to_text(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


class McpSchema(JsonRpcSchema):
    """
    MCP-over-MAGPIE schema.

    Extends JsonRpcSchema with built-in handlers for the MCP handshake and
    tool-dispatch protocol (``initialize``, ``tools/list``, ``tools/call``).
    Any method registered via ``register()`` or ``@schema.method()`` is
    automatically exposed as an MCP tool.

    Usage::

        schema = McpSchema(name="qtrobot")

        @schema.method()
        def move_motor(motor: str, angle: float) -> dict:
            \"\"\"Move a robot motor to a specific angle.\"\"\"
            return {"success": True}

        server = MqttRpcResponder(conn, service_name="robot-01", schema=schema)
        while True:
            server.handle_once(timeout=1.0)
    """

    # Built-in MCP method names — never exposed as tools
    _BUILTIN_METHODS = frozenset({
        "initialize",
        "notifications/initialized",
        "notifications/cancelled",
        "tools/list",
        "tools/call",
        "ping",
    })

    def __init__(self, name: str = "magpie", version: str = "1.0.0"):
        super().__init__()
        self._server_name = name
        self._server_version = version
        self._tools: dict = {}  # tool_name → {"description", "input_schema"}

        # Register built-in MCP handlers directly into _methods so they
        # bypass the tools-tracking logic in our overridden register().
        self._methods["initialize"] = {
            "func": self._mcp_initialize,
            "description": "MCP initialize handshake",
            "input_schema": {"type": "object"},
        }
        self._methods["notifications/initialized"] = {
            "func": lambda **_: None,
            "description": "",
            "input_schema": {"type": "object"},
        }
        self._methods["notifications/cancelled"] = {
            "func": lambda **_: None,
            "description": "",
            "input_schema": {"type": "object"},
        }
        self._methods["ping"] = {
            "func": lambda **_: {},
            "description": "MCP ping",
            "input_schema": {"type": "object"},
        }
        self._methods["tools/list"] = {
            "func": self._mcp_tools_list,
            "description": "List available tools",
            "input_schema": {"type": "object"},
        }
        self._methods["tools/call"] = {
            "func": self._mcp_tools_call,
            "description": "Call a tool by name",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name":      {"type": "string"},
                    "arguments": {"type": "object"},
                },
                "required": ["name"],
            },
        }

    # ------------------------------------------------------------------
    # Registration — tracks user methods as tools
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        func: Callable,
        description: str = None,
        input_schema: dict = None,
    ) -> None:
        super().register(name, func, description=description, input_schema=input_schema)
        if name not in self._BUILTIN_METHODS:
            import inspect
            self._tools[name] = {
                "description": description or (inspect.getdoc(func) or ""),
                "input_schema": input_schema or _infer_input_schema(func),
            }

    # ------------------------------------------------------------------
    # Built-in MCP handlers
    # ------------------------------------------------------------------

    def _mcp_initialize(self, **kwargs) -> dict:
        return {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": self._server_name,
                "version": self._server_version,
            },
        }

    def _mcp_tools_list(self, **kwargs) -> dict:
        tools = [
            {
                "name": tool_name,
                "description": meta["description"],
                "inputSchema": meta["input_schema"],
            }
            for tool_name, meta in self._tools.items()
        ]
        return {"tools": tools}

    def _mcp_tools_call(self, name: str = None, arguments: dict = None) -> dict:
        if not name:
            raise ValueError("'name' is required")
        entry = self._methods.get(name)
        if entry is None or name not in self._tools:
            return {
                "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
                "isError": True,
            }
        try:
            result = entry["func"](**(arguments or {}))
            return {
                "content": [{"type": "text", "text": _to_text(result)}],
                "isError": False,
            }
        except Exception as e:
            return {
                "content": [{"type": "text", "text": str(e)}],
                "isError": True,
            }
