"""
mqtt_mcp_server.py — Robot side: serve MCP tools over MAGPIE/MQTT

Run first:
    python examples/mqtt_mcp_server.py

Then in another terminal:
    python examples/mqtt_mcp_client.py

Requirements:
    pip install luxai-magpie[mqtt]
    A running MQTT broker at localhost:1883 (e.g. Mosquitto)
"""

from luxai.magpie.schema import McpSchema
from luxai.magpie.transport import MqttRpcResponder
from luxai.magpie.transport.mqtt import MqttConnection
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


# --- Connect and serve ---

conn = MqttConnection("mqtt://localhost:1883")
conn.connect()

responder = MqttRpcResponder(conn, service_name="math-robot", schema=schema)

Logger.info("Math robot MCP server running. Press Ctrl-C to stop.")
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
    conn.disconnect()
