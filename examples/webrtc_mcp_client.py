"""
webrtc_mcp_client.py — Cloud/agent side: use FastMCP Client over MAGPIE/WebRTC

MQTT is used only for the initial WebRTC signaling handshake.  Once the P2P
data channel is established all RPC traffic flows directly — lower latency
than pure MQTT.

Run after starting webrtc_mcp_server.py.

Requirements:
    pip install luxai-magpie[webrtc,mqtt,mcp]
    A running MQTT broker for signaling (or use with_zmq for LAN)

Usage:
    python examples/webrtc_mcp_client.py
"""

import asyncio

from fastmcp import Client
from fastmcp.exceptions import ToolError

from luxai.magpie.adapters.mcp import McpTransport
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcRequester
from luxai.magpie.utils import Logger

Logger.set_level("INFO")

BROKER_URI   = "mqtt://localhost:1883"        # MQTT used only for signaling
SESSION_ID   = "magpie/examples/webrtc-mcp"       # must match server
SERVICE_NAME = "math-robot"


async def main():
    # For broker-less LAN use with_zmq() instead:
    # conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", SESSION_ID, bind=False)
    conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID)
    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    req = WebRTCRpcRequester(conn, service_name=SERVICE_NAME)

    transport = McpTransport(req, timeout=10.0)
    async with Client(transport) as client:
        tools = await client.list_tools()
        Logger.info("Available tools:")
        for tool in tools:
            Logger.info(f"  {tool.name}: {tool.description}")

        result = await client.call_tool("add", {"a": 10, "b": 32})
        Logger.info(f"add(10, 32) = {result.content[0].text}")

        result = await client.call_tool("multiply", {"a": 6, "b": 7})
        Logger.info(f"multiply(6, 7) = {result.content[0].text}")

        result = await client.call_tool("divide", {"a": 22, "b": 7})
        Logger.info(f"divide(22, 7) = {result.content[0].text}")

        try:
            result = await client.call_tool("divide", {"a": 1, "b": 0})
            Logger.info(f"divide(1, 0) = {result.content[0].text}")
        except ToolError as e:
            Logger.warning(f"divide(1, 0) raised ToolError: {e}")

    req.close()
    conn.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
