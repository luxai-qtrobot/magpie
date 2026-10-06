"""Bounded, in-memory implementation of the MAGPIE HTTP signaling contract.

The relay forwards opaque bytes and has no dependency on MAGPIE. It is meant
for one process and one Azure Container Apps replica.
"""

from __future__ import annotations

import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import parse_qs


_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~-]{2,127}$")


@dataclass(frozen=True)
class HTTPResult:
    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


class _Mailbox:
    def __init__(self):
        self.messages = deque()  # (sequence, opaque bytes)
        self.announcement = None
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
    """Thread-safe signaling rooms with explicit resource limits."""

    def __init__(
        self,
        *,
        lease_seconds=90.0,
        max_sessions=16,
        max_peers_per_session=4,
        max_queued_messages=256,
    ):
        self.rooms = {}  # session -> peer -> _Mailbox
        self.changed = threading.Condition()
        self.lease_seconds = lease_seconds
        self.max_sessions = max_sessions
        self.max_peers_per_session = max_peers_per_session
        self.max_queued_messages = max_queued_messages

    def _prune(self):
        now = time.monotonic()
        for session, room in list(self.rooms.items()):
            for peer, box in list(room.items()):
                if now - box.last_seen > self.lease_seconds:
                    del room[peer]
            if not room:
                del self.rooms[session]

    def _enqueue(self, box, payload):
        while len(box.messages) >= self.max_queued_messages:
            box.messages.popleft()
        box.messages.append((box.next_sequence, payload))
        box.next_sequence += 1

    def join(self, session, peer, announcement=b""):
        """Join a room, returning False when an admission limit is reached."""
        with self.changed:
            self._prune()
            room = self.rooms.get(session)
            if room is None:
                if self.max_sessions and len(self.rooms) >= self.max_sessions:
                    return False
                room = self.rooms.setdefault(session, {})

            box = room.get(peer)
            if box is None:
                if (
                    self.max_peers_per_session
                    and len(room) >= self.max_peers_per_session
                ):
                    return False
                box = room[peer] = _Mailbox()
                for other_peer, other_box in room.items():
                    if other_peer != peer and other_box.announcement:
                        self._enqueue(box, other_box.announcement)

            box.last_seen = time.monotonic()
            if announcement and announcement != box.announcement:
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
                return True
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

    def stats(self):
        with self.changed:
            self._prune()
            return {
                "sessions": len(self.rooms),
                "peers": sum(len(room) for room in self.rooms.values()),
            }


class SignalingHTTP:
    """Framework-neutral handler for the MAGPIE HTTP signaling contract."""

    def __init__(self, relay=None, max_message_bytes=256 * 1024):
        self.relay = relay if relay is not None else InMemoryRelay()
        self.max_message_bytes = max_message_bytes

    def handle(self, method, path, query="", headers=None, body=b""):
        parts = path.strip("/").split("/")
        if (
            len(parts) not in (4, 5)
            or parts[0] != "sessions"
            or parts[2] != "peers"
            or not _valid_id(parts[1])
            or not _valid_id(parts[3])
            or (len(parts) == 5 and parts[4] != "messages")
        ):
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
            header_values = {key.lower(): value for key, value in (headers or {}).items()}
            message_id = header_values.get("x-magpie-message-id")
            if not message_id or len(message_id) > 128:
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
                200,
                payload,
                {
                    "Content-Type": "application/octet-stream",
                    "X-Magpie-Sequence": str(sequence),
                },
            )
        return HTTPResult(405)


def _valid_id(value):
    return bool(_ID_PATTERN.fullmatch(value))


def relative_path(path, prefix):
    prefix = prefix.rstrip("/")
    if prefix and (path == prefix or path.startswith(prefix + "/")):
        return path[len(prefix):] or "/"
    return path
