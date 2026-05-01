from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

try:
    import anyio
    from pydantic import TypeAdapter as _TypeAdapter
    from mcp import types as _mcp_types, ClientSession
    from mcp.shared.message import SessionMessage as _SessionMessage
    from fastmcp.client.transports.base import ClientTransport
    _JSONRPC_ADAPTER = _TypeAdapter(_mcp_types.JSONRPCMessage)
except ImportError as _err:
    raise ImportError(
        "McpTransport requires 'fastmcp': pip install luxai-magpie[mcp]"
    ) from _err

from luxai.magpie.transport.rpc_requester import RpcRequester
from luxai.magpie.utils.logger import Logger


class McpTransport(ClientTransport):
    """
    FastMCP ``ClientTransport`` backed by any MAGPIE ``RpcRequester``.

    Works with ZMQ, MQTT, WebRTC — or any future transport — without
    modification.  The caller creates and owns the requester (and its
    underlying connection); ``McpTransport`` borrows it and never closes it.

    Protocol flow::

        FastMCP Client ──JSON-RPC──► McpTransport (bridge)
                                          │  MAGPIE request/reply
                                          ▼
                                     RpcResponder + McpSchema (robot)

    Usage::

        # MQTT
        conn = MqttConnection("mqtt://broker:1883", options=MqttOptions(...))
        conn.connect()
        req = MqttRpcRequester(conn, service_name="robot-01")

        async with Client(McpTransport(req)) as client:
            tools = await client.list_tools()
            result = await client.call_tool("add", {"a": 3, "b": 4})

        req.close()
        conn.disconnect()

        # ZMQ — identical pattern, no new class needed
        req = ZMQRpcRequester("tcp://robot:5555")
        async with Client(McpTransport(req)) as client:
            ...
        req.close()
    """

    def __init__(self, requester: RpcRequester, timeout: float = 30.0):
        """
        Args:
            requester: Any MAGPIE ``RpcRequester`` (ZMQ, MQTT, WebRTC, ...).
                       The caller is responsible for closing it after use.
            timeout:   Per-call timeout in seconds (default 30).
        """
        self._requester = requester
        self._timeout = timeout

    # ------------------------------------------------------------------
    # FastMCP ClientTransport interface
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def connect_session(self, **session_kwargs: Any):
        """
        Yield an active ``mcp.ClientSession`` connected to the MAGPIE
        responder through the bridge loop.

        Bridge logic:
        - JSON-RPC *requests* (have ``id``) are forwarded to the robot via
          ``requester.call()`` and the reply is put back on the read stream.
        - MCP *notifications* (no ``id``) are dropped — MAGPIE always expects
          a reply, but notifications expect none.
        """
        Logger.debug(
            f"McpTransport: session starting via {type(self._requester).__name__}"
        )

        read_stream_writer, read_stream = anyio.create_memory_object_stream(max_buffer_size=64)
        write_stream, write_stream_reader = anyio.create_memory_object_stream(max_buffer_size=64)

        async def _bridge():
            async with write_stream_reader, read_stream_writer:
                async for msg in write_stream_reader:
                    msg_dict = json.loads(
                        msg.message.model_dump_json(by_alias=True, exclude_none=True)
                    )
                    if "id" not in msg_dict:
                        continue
                    try:
                        reply_dict = await anyio.to_thread.run_sync(
                            lambda d=msg_dict: self._requester.call(d, timeout=self._timeout)
                        )
                        if reply_dict is not None:
                            reply_msg = _SessionMessage(
                                message=_JSONRPC_ADAPTER.validate_python(reply_dict)
                            )
                            await read_stream_writer.send(reply_msg)
                    except Exception as exc:
                        Logger.debug(f"McpTransport: call error: {exc}")
                        await read_stream_writer.send(exc)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_bridge)
            async with ClientSession(read_stream, write_stream, **session_kwargs) as session:
                yield session
            tg.cancel_scope.cancel()

        Logger.debug("McpTransport: session ended.")
