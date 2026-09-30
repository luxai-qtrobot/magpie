"""Framework-neutral HTTP signaling protocol for WebRTC peers.

This module uses only the Python standard library and never reads signaling
payloads. Copy this directory into an application that does not install MAGPIE.
The in-memory store is for a single process; a multi-worker service needs a
shared implementation of the same join/send/receive/leave operations.
"""

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import parse_qs


@dataclass(frozen=True)
class HTTPResult:
    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


class _Mailbox:
    def __init__(self):
        self.messages = deque()  # (sequence, opaque bytes)
        self.announcement = None  # latest opaque join announcement, if any
        self.next_sequence = 1
        self.seen_ids = set()
        self.seen_order = deque()
        self.last_seen = time.monotonic()

    def remember(self, message_id):
        if message_id in self.seen_ids:
            return False
        self.seen_ids.add(message_id)
        self.seen_order.append(message_id)
        if len(self.seen_order) > 256:
            self.seen_ids.discard(self.seen_order.popleft())
        return True


class InMemoryRelay:
    """Thread-safe, single-process mailbox store for a signaling room."""

    def __init__(self, lease_seconds=90.0):
        self.rooms = {}  # session -> peer -> _Mailbox
        self.changed = threading.Condition()
        self.lease_seconds = lease_seconds

    def _prune(self):
        now = time.monotonic()
        for session, room in list(self.rooms.items()):
            for peer, box in list(room.items()):
                if now - box.last_seen > self.lease_seconds:
                    del room[peer]
            if not room:
                del self.rooms[session]

    @staticmethod
    def _enqueue(box, payload):
        box.messages.append((box.next_sequence, payload))
        box.next_sequence += 1

    def join(self, session, peer, announcement=b""):
        """Atomically exchange a new peer's announcement with room members."""
        with self.changed:
            self._prune()
            room = self.rooms.setdefault(session, {})
            box = room.get(peer)
            if box is None:
                box = room[peer] = _Mailbox()
                for other_peer, other_box in room.items():
                    if other_peer != peer and other_box.announcement:
                        self._enqueue(box, other_box.announcement)
            box.last_seen = time.monotonic()
            if announcement and announcement != box.announcement:
                # A peer that registered without an announcement may add one
                # later. Repeating the same PUT remains idempotent.
                box.announcement = bytes(announcement)
                for other_peer, other_box in room.items():
                    if other_peer != peer:
                        self._enqueue(other_box, box.announcement)
            self.changed.notify_all()
            return True

    def leave(self, session, peer):
        with self.changed:
            room = self.rooms.get(session, {})
            room.pop(peer, None)
            if not room:
                self.rooms.pop(session, None)
            self.changed.notify_all()

    def send(self, session, peer, message_id, payload):
        with self.changed:
            self._prune()
            room = self.rooms.get(session, {})
            sender = room.get(peer)
            if sender is None:
                return False
            sender.last_seen = time.monotonic()
            if not sender.remember(message_id):
                return True  # retried POST
            for other_peer, box in room.items():
                if other_peer != peer:
                    self._enqueue(box, payload)
            self.changed.notify_all()
            return True

    def receive(self, session, peer, after, wait):
        deadline = time.monotonic() + wait
        with self.changed:
            while True:
                self._prune()
                box = self.rooms.get(session, {}).get(peer)
                if box is None:
                    return None, None
                box.last_seen = time.monotonic()
                while box.messages and box.messages[0][0] <= after:
                    box.messages.popleft()
                if box.messages:
                    return box.messages[0]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return 0, b""
                self.changed.wait(remaining)


class SignalingHTTP:
    """Handle the complete HTTP wire contract without a web framework.

    ``handle`` accepts a path relative to the signaling mount point. The
    adapters supply request fields and translate ``HTTPResult`` to their web
    server's response type. Authentication belongs to the hosting application.
    """

    def __init__(self, relay=None, max_message_bytes=1024 * 1024):
        self.relay = relay if relay is not None else InMemoryRelay()
        self.max_message_bytes = max_message_bytes

    def handle(self, method, path, query="", headers=None, body=b""):
        parts = path.strip("/").split("/")
        if (len(parts) not in (4, 5) or parts[0] != "sessions"
                or parts[2] != "peers" or not parts[1] or not parts[3]
                or (len(parts) == 5 and parts[4] != "messages")):
            return HTTPResult(404)
        session, peer = parts[1], parts[3]
        is_messages = len(parts) == 5

        if method == "PUT" and not is_messages:
            if len(body) > self.max_message_bytes:
                return HTTPResult(413)
            joined = self.relay.join(session, peer, body)
            return HTTPResult(
                204 if joined else 409,
                headers={"X-Magpie-Join-Announcements": "1"},
            )
        if method == "DELETE" and not is_messages:
            self.relay.leave(session, peer)
            return HTTPResult(204)
        if method == "POST" and is_messages:
            if len(body) > self.max_message_bytes:
                return HTTPResult(413)
            header_values = {k.lower(): v for k, v in (headers or {}).items()}
            message_id = header_values.get("x-magpie-message-id")
            if not message_id:
                return HTTPResult(400)
            return HTTPResult(
                204 if self.relay.send(session, peer, message_id, body) else 404
            )
        if method == "GET" and is_messages:
            params = parse_qs(query)
            try:
                after = int(params.get("after", ["0"])[0])
                wait = float(params.get("wait", ["20"])[0])
            except (TypeError, ValueError):
                return HTTPResult(400)
            if after < 0 or not math.isfinite(wait) or not 0 <= wait <= 30:
                return HTTPResult(400)
            sequence, payload = self.relay.receive(session, peer, after, wait)
            if sequence is None:
                return HTTPResult(404)
            if sequence == 0:
                return HTTPResult(204)
            return HTTPResult(
                200, payload,
                {"Content-Type": "application/octet-stream",
                 "X-Magpie-Sequence": str(sequence)},
            )
        return HTTPResult(405)


def relative_path(path, prefix):
    """Accept both a mounted path and a standalone app's configured prefix."""
    prefix = prefix.rstrip("/")
    if prefix and (path == prefix or path.startswith(prefix + "/")):
        return path[len(prefix):] or "/"
    return path
