"""
zmq_mcp_server.py — Robot side: serve MCP tools over MAGPIE/ZMQ

Run first:
    python examples/zmq_mcp_server.py

Then in another terminal:
    python examples/zmq_mcp_client.py

Requirements:
    pip install luxai-magpie   (ZMQ is a core dependency)
"""

from luxai.magpie.schema import McpSchema
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.utils import Logger

Logger.set_level("INFO")

# --- Build the MCP schema ---

schema = McpSchema(name="math-robot", version="1.0.0")


@schema.method()
def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


@schema.method()
def subtract(a: float, b: float) -> float:
    """Subtract b from a."""
    return a - b


@schema.method()
def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b


@schema.method()
def divide(a: float, b: float) -> float:
    """Divide a by b. Raises an error if b is zero."""
    if b == 0:
        raise ValueError("Division by zero")
    return a / b


# --- Bind and serve ---

responder = ZMQRpcResponder("tcp://*:5556", schema=schema)

Logger.info("Math robot MCP server running on tcp://*:5556. Press Ctrl-C to stop.")
try:
    while True:
        try:
            responder.handle_once(timeout=1.0)
        except TimeoutError:
            pass
except KeyboardInterrupt:
    pass
finally:
    responder.close()
