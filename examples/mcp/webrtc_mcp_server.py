"""
webrtc_mcp_server.py — Robot side: serve MCP tools over MAGPIE/WebRTC

WebRTC gives a direct P2P data channel after the initial signaling handshake
via MQTT (or ZMQ for LAN use).  No broker in the hot path once connected.

Run first:
    python examples/webrtc_mcp_server.py

Then in another terminal:
    python examples/webrtc_mcp_client.py

Requirements:
    pip install luxai-magpie[webrtc,mqtt]
    A running MQTT broker for signaling (or use with_zmq for LAN)
"""

from luxai.magpie.schema import McpSchema
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcResponder
from luxai.magpie.utils import Logger

Logger.set_level("INFO")

BROKER_URI   = "mqtt://localhost:1883"        # MQTT used only for signaling
SESSION_ID   = "magpie/examples/webrtc-mcp"       # must match client
SERVICE_NAME = "math-robot"

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

# For broker-less LAN use with_zmq() instead:
# conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", SESSION_ID, bind=True)
conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID)
if not conn.connect():
    raise SystemExit("WebRTC handshake timed out.")

responder = WebRTCRpcResponder(conn, service_name=SERVICE_NAME, schema=schema)

Logger.info("Math robot MCP server running over WebRTC. Press Ctrl-C to stop.")
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
