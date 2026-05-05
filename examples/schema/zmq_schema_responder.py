"""
ZMQ RPC responder with JsonRpcSchema.

Demonstrates two ways to define and attach handlers:

  Way A + D — load API from JSON, attach handlers separately
  Way C     — decorator defines shape + handler together

Run this first, then run zmq_schema_requester.py.
"""
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.schema import JsonRpcSchema
from luxai.magpie.utils import Logger


# ── Way A: load API from a Python list, attach handlers separately ──

schema = JsonRpcSchema.from_json([
    {
        "name": "add",
        "description": "Add two numbers",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
    {
        "name": "sub",
        "description": "Subtract b from a",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
])


# ── Way D: attach handlers to pre-defined methods ───────────────────

@schema.handler("add")
def handle_add(a, b):
    return a + b


@schema.handler("sub")
def handle_sub(a, b):
    return a - b


# ── Way C: decorator defines shape + handler together ────────────────

@schema.method()
def mul(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b


@schema.method()
def div(a: float, b: float) -> float:
    """Divide a by b."""
    if b == 0:
        raise ValueError("division by zero")
    return a / b


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    server = ZMQRpcResponder("tcp://*:5556", schema=schema)
    Logger.info("zmq_schema_responder: listening on tcp://*:5556")

    while True:
        try:
            server.handle_once(timeout=1.0)
        except KeyboardInterrupt:
            Logger.info("stopping...")
            server.close()
            break
