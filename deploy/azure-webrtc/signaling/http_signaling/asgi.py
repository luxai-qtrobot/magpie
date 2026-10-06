"""Small ASGI adapter for the framework-neutral signaling protocol."""

import asyncio

from .relay import HTTPResult, relative_path


class SignalingASGI:
    def __init__(self, protocol, prefix="/signal"):
        self.protocol = protocol
        self.prefix = prefix

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            raise RuntimeError("SignalingASGI supports HTTP requests only")

        body = bytearray()
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                return
            body.extend(event.get("body", b""))
            if len(body) > self.protocol.max_message_bytes:
                await self._send(send, HTTPResult(413))
                return
            if not event.get("more_body", False):
                break

        path = scope["path"]
        root_path = scope.get("root_path", "").rstrip("/")
        if root_path and (path == root_path or path.startswith(root_path + "/")):
            path = path[len(root_path):] or "/"
        path = relative_path(path, self.prefix)
        headers = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope["headers"]
        }
        result = await asyncio.to_thread(
            self.protocol.handle,
            scope["method"],
            path,
            scope.get("query_string", b"").decode("latin-1"),
            headers,
            bytes(body),
        )
        await self._send(send, result)

    @staticmethod
    async def _send(send, result):
        headers = {
            "Cache-Control": "no-store",
            "Content-Length": str(len(result.body)),
        }
        headers.update(result.headers)
        await send({
            "type": "http.response.start",
            "status": result.status,
            "headers": [
                (key.lower().encode("ascii"), value.encode("latin-1"))
                for key, value in headers.items()
            ],
        })
        await send({"type": "http.response.body", "body": result.body})
