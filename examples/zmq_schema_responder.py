"""
ZMQ RPC responder with JsonRpcSchema.

Demonstrates two ways to define and attach handlers:

  Way A + D — load API from dict, attach handlers separately
  Way C     — decorator defines shape + handler together

Run this first, then run zmq_schema_requester.py.
"""
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.schema import JsonRpcSchema
from luxai.magpie.utils import Logger


# ── Way A: define API from dict (no handlers yet) ───────────────────
MATH_API = {
    "add": {
        "description": "Add two numbers",
        "params": {
            "a": {"type": "number", "required": True},
            "b": {"type": "number", "required": True},
        },
    },
    "sub": {
        "description": "Subtract b from a",
        "params": {
            "a": {"type": "number", "required": True},
            "b": {"type": "number", "required": True},
        },
    },
}

schema = JsonRpcSchema.from_dict(MATH_API)


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
