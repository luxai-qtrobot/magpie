"""
zmq_mcp_client.py — Cloud/agent side: use FastMCP Client over MAGPIE/ZMQ

Run after starting zmq_mcp_server.py.

Requirements:
    pip install luxai-magpie[mcp]

Usage:
    python examples/zmq_mcp_client.py
"""

import asyncio

from fastmcp import Client
from fastmcp.exceptions import ToolError

from luxai.magpie.adapters.mcp import McpTransport
from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.utils import Logger

Logger.set_level("DEBUG")


async def main():
    req = ZMQRpcRequester("tcp://127.0.0.1:5556")

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


if __name__ == "__main__":
    asyncio.run(main())
