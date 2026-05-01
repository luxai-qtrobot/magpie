"""
mqtt_mcp_client.py — Cloud/agent side: use FastMCP Client over MAGPIE/MQTT

Run after starting mqtt_mcp_responder.py.

Requirements:
    pip install luxai-magpie[mqtt,mcp]
    A running MQTT broker at localhost:1883

Usage:
    python examples/mqtt_mcp_client.py
"""

import asyncio

from fastmcp import Client
from fastmcp.exceptions import ToolError

from luxai.magpie.adapters.mcp import McpTransport
from luxai.magpie.transport.mqtt import MqttConnection
from luxai.magpie.transport import MqttRpcRequester
from luxai.magpie.utils import Logger

Logger.set_level("INFO")


async def main():
    conn = MqttConnection("mqtt://localhost:1883")
    conn.connect()
    req = MqttRpcRequester(conn, service_name="math-robot")

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
