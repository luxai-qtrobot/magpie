"""
ZMQ RPC requester with JsonRpcSchema.

Demonstrates all schema definition styles on the requester side and
both call styles (proxy and base call). Run zmq_schema_responder.py first.
"""
import time

from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.schema import JsonRpcSchema, JsonRpcError
from luxai.magpie.utils import Logger


# Any of these produce an equivalent schema — pick one:

# Way A — from a Python list
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
    {
        "name": "mul",
        "description": "Multiply two numbers",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
    {
        "name": "div",
        "description": "Divide a by b",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
])

# Way B — programmatic register
# schema = JsonRpcSchema()
# schema.register(name="add", input_schema=ab)
# schema.register(name="sub", input_schema=ab)
# schema.register(name="mul", input_schema=ab)
# schema.register(name="div", input_schema=ab)

# Way C — decorator with stub bodies
# schema = JsonRpcSchema()
# @schema.method()
# def add(a: float, b: float) -> float: ...
# @schema.method()
# def sub(a: float, b: float) -> float: ...
# @schema.method()
# def mul(a: float, b: float) -> float: ...
# @schema.method()
# def div(a: float, b: float) -> float: ...


if __name__ == "__main__":
    Logger.set_level("INFO")

    client = ZMQRpcRequester("tcp://127.0.0.1:5556", schema=schema)

    try:
        # ── proxy style ──────────────────────────────────────────────
        Logger.info(f"add proxy:  3 + 4 = {client.add(a=3, b=4)}")
        time.sleep(0.3)

        Logger.info(f"sub proxy:  10 - 3 = {client.sub(a=10, b=3)}")
        time.sleep(0.3)

        Logger.info(f"mul proxy:  6 * 7 = {client.mul(a=6, b=7)}")
        time.sleep(0.3)

        Logger.info(f"div proxy:  10 / 4 = {client.div(a=10, b=4)}")
        time.sleep(0.3)

        # ── proxy with explicit timeout ───────────────────────────────
        Logger.info(f"add (_timeout): 100 + 200 = {client.add(a=100, b=200, _timeout=5.0)}")
        time.sleep(0.3)

        # ── base call style — equivalent ─────────────────────────────
        Logger.info(f"add base call: 1 + 2 = {client.call('add', a=1, b=2)}")
        time.sleep(0.3)

        # ── error from server (division by zero) ──────────────────────
        try:
            client.div(a=10, b=0)
        except JsonRpcError as e:
            Logger.warning(f"expected server error {e.code}: {e.message}")

        # ── unknown method ────────────────────────────────────────────
        try:
            client.call("unknown")
        except JsonRpcError as e:
            Logger.warning(f"expected error {e.code}: {e.message}")

    except KeyboardInterrupt:
        Logger.info("stopping...")
    finally:
        client.close()
