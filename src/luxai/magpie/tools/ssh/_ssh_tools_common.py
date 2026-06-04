"""Shared helpers for Magpie SSH tunnel tools."""
import shlex
import socket
import sys
import threading
from typing import Optional

from luxai.magpie.utils.logger import Logger

SSH_TOPIC_PREFIX = "magpie/ssh"
SSH_STREAM_TOPIC_UP   = "ssh/up"    # client → server
SSH_STREAM_TOPIC_DOWN = "ssh/down"  # server → client

CHUNK_SIZE = 4096


def mqtt_up_topic(node_id: str, session_ulid: str) -> str:
    return f"{SSH_TOPIC_PREFIX}/{node_id}/{session_ulid}/up"


def mqtt_down_topic(node_id: str, session_ulid: str) -> str:
    return f"{SSH_TOPIC_PREFIX}/{node_id}/{session_ulid}/down"


def mqtt_wildcard_up(node_id: str) -> str:
    return f"{SSH_TOPIC_PREFIX}/{node_id}/+/up"


def extract_session_ulid(topic: str, node_id: str) -> Optional[str]:
    """Extract session ULID from a full magpie/ssh/<node_id>/<ulid>/up topic."""
    prefix = f"{SSH_TOPIC_PREFIX}/{node_id}/"
    if not topic.startswith(prefix):
        return None
    rest = topic[len(prefix):]
    parts = rest.split("/")
    if len(parts) == 2 and parts[1] == "up":
        return parts[0]
    return None


# ---------------------------------------------------------------------------
# Byte bridge helpers
# ---------------------------------------------------------------------------

def bridge_socket_to_writer(sock, writer, topic: str, stop_event: threading.Event) -> None:
    """Read from socket, write to StreamWriter. Sets stop_event on EOF or error."""
    try:
        while not stop_event.is_set():
            try:
                data = sock.recv(CHUNK_SIZE)
            except OSError:
                break
            if not data:
                break
            writer.write(data, topic=topic)
    finally:
        stop_event.set()


def bridge_reader_to_socket(reader, sock, stop_event: threading.Event) -> None:
    """Read from StreamReader, write to socket. Sets stop_event on EOF or error."""
    try:
        while not stop_event.is_set():
            try:
                result = reader.read(timeout=1.0)
            except TimeoutError:
                continue
            except Exception as e:
                Logger.debug(f"ssh bridge reader error: {e}")
                break
            if result is None:
                break
            data, _ = result
            if not data:
                continue
            try:
                sock.sendall(data)
            except OSError:
                break
    finally:
        stop_event.set()


def bridge_stdin_to_writer(writer, topic: str, stop_event: threading.Event) -> None:
    """Read raw bytes from stdin, write to StreamWriter. Sets stop_event on EOF."""
    import io
    stdin = sys.stdin.buffer
    # read1() returns as soon as any bytes are available (no blocking until full).
    # Fall back to read() on platforms where read1() is unavailable.
    reader_fn = getattr(stdin, "read1", None) or stdin.read
    try:
        while not stop_event.is_set():
            try:
                data = reader_fn(CHUNK_SIZE)
            except OSError:
                break
            if not data:
                break
            writer.write(data, topic=topic)
    finally:
        stop_event.set()


def bridge_reader_to_stdout(reader, stop_event: threading.Event) -> None:
    """Read from StreamReader, write raw bytes to stdout. Sets stop_event on EOF."""
    try:
        while not stop_event.is_set():
            try:
                result = reader.read(timeout=1.0)
            except TimeoutError:
                continue
            except Exception as e:
                Logger.debug(f"ssh bridge reader→stdout error: {e}")
                break
            if result is None:
                break
            data, _ = result
            if not data:
                continue
            try:
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
            except OSError:
                break
    finally:
        stop_event.set()


def connect_sshd(host: str, port: int) -> socket.socket:
    """Connect to local sshd and return the socket. Raises on failure."""
    sock = socket.create_connection((host, port), timeout=10.0)
    sock.settimeout(None)
    return sock


# ---------------------------------------------------------------------------
# ProxyCommand string reconstruction helpers
# ---------------------------------------------------------------------------

def build_proxy_command(prog: str, node_id: str, extra_args: list) -> str:
    """Build a ProxyCommand string for use in ~/.ssh/config or -o ProxyCommand=."""
    parts = [prog, "--proxy"] + extra_args + [node_id]
    return " ".join(shlex.quote(p) for p in parts)
