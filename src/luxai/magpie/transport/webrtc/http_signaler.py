"""HTTP long-poll signaling for two WebRTC peers.

The HTTP server only routes opaque bytes.  The wire contract is documented in
``docs/webrtc-http-signaling.md``. The server does not require MAGPIE. All
network I/O runs in worker threads so aiortc's loop is never blocked by an
HTTP request.
"""

import base64
import queue
import threading
import uuid
from typing import Callable, Mapping, Optional
from urllib.parse import urlsplit

from luxai.magpie.utils.logger import Logger

from .webrtc_signaler import WebRtcSignaler


class HttpSignaler(WebRtcSignaler):
    """Exchange MAGPIE signaling messages through an HTTP mailbox service.

    ``headers`` are sent on every request. ``headers_provider`` is called
    before each request, making refreshed bearer tokens possible; its values
    override fixed headers. The provider may be called concurrently by the
    send and poll workers. An injected ``http_client`` is borrowed and is
    never closed by this class. Both paths require ``httpx``.
    """

    def __init__(
        self,
        base_url: str,
        session_id: str,
        *,
        participant_id: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        headers_provider: Optional[Callable[[], Mapping[str, str]]] = None,
        http_client=None,
        poll_wait: float = 20.0,
        request_timeout: float = 10.0,
    ):
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "HttpSignaler requires httpx. Install with: "
                "pip install 'luxai-magpie[webrtc]'"
            ) from exc

        parsed = urlsplit(base_url)
        if (parsed.scheme not in ("http", "https") or not parsed.netloc
                or parsed.query or parsed.fragment):
            raise ValueError("base_url must be an http:// or https:// URL without a query or fragment")
        if not session_id:
            raise ValueError("session_id must not be empty")
        if not participant_id:
            participant_id = uuid.uuid4().hex
        if poll_wait <= 0 or request_timeout <= 0:
            raise ValueError("poll_wait and request_timeout must be positive")

        self._httpx = httpx
        self._session_id = session_id
        self._participant_id = participant_id
        self._headers = dict(headers or {})
        self._headers_provider = headers_provider
        self._poll_wait = poll_wait
        self._request_timeout = request_timeout
        self._peer_url = (
            base_url.rstrip("/")
            + "/sessions/" + self._encode_id(session_id)
            + "/peers/" + self._encode_id(participant_id)
        )
        self._messages_url = self._peer_url + "/messages"
        self._owns_client = http_client is None
        self._client = http_client if http_client is not None else httpx.Client()
        self._callback: Optional[Callable[[bytes], None]] = None
        self._cursor = 0
        self._closed = False
        self._stop = threading.Event()
        self._subscribed = threading.Event()
        self._outgoing: queue.Queue = queue.Queue()
        self._poll_thread: Optional[threading.Thread] = None
        self._send_thread: Optional[threading.Thread] = None

        try:
            self._register()
        except Exception:
            if self._owns_client:
                self._client.close()
            raise

        self._send_thread = threading.Thread(
            target=self._send_loop, name="HttpSignalerSend", daemon=True
        )
        self._send_thread.start()

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def participant_id(self) -> str:
        return self._participant_id

    @staticmethod
    def _encode_id(value: str) -> str:
        """Use one safe path segment even when a user ID contains a slash."""
        return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")

    def publish(self, payload: bytes) -> None:
        if self._closed:
            raise RuntimeError("HttpSignaler is disconnected")
        self._outgoing.put((uuid.uuid4().hex, bytes(payload)))

    def subscribe(self, callback: Callable[[bytes], None]) -> None:
        if self._closed:
            raise RuntimeError("HttpSignaler is disconnected")
        self._callback = callback
        self._subscribed.set()
        if self._poll_thread is None or not self._poll_thread.is_alive():
            self._poll_thread = threading.Thread(
                target=self._poll_loop, name="HttpSignalerPoll", daemon=True
            )
            self._poll_thread.start()

    def unsubscribe(self) -> None:
        self._callback = None
        self._subscribed.clear()

    def disconnect(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self._subscribed.set()  # wake the poll worker if idle
        self._outgoing.put(None)
        try:
            self._request("DELETE", self._peer_url)
        except Exception:
            pass  # server may already be unavailable
        for thread in (self._poll_thread, self._send_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)
        if self._owns_client:
            self._client.close()

    def _request(self, method: str, url: str, **kwargs):
        request_headers = self._httpx.Headers(self._headers)
        if self._headers_provider is not None:
            request_headers.update(self._headers_provider() or {})
        request_headers.update(kwargs.pop("headers", {}))
        return self._client.request(
            method, url, headers=request_headers,
            timeout=kwargs.pop("timeout", self._request_timeout), **kwargs
        )

    def _register(self) -> None:
        response = self._request("PUT", self._peer_url)
        if response.status_code == 409:
            raise self._httpx.HTTPStatusError(
                f"HTTP signaling session {self._session_id!r} already has two "
                "participants. Stop an old peer, wait for its lease to expire, "
                "restart the example relay, or choose a new session ID.",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        self._cursor = 0  # relay may have restarted its sequence numbers

    def _send_loop(self) -> None:
        while not self._stop.is_set():
            item = self._outgoing.get()
            if item is None:
                return
            message_id, payload = item
            delay = 0.2
            while not self._stop.is_set():
                try:
                    response = self._request(
                        "POST", self._messages_url, content=payload,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "X-Magpie-Message-Id": message_id,
                        },
                    )
                    if self._stop.is_set():
                        return
                    if response.status_code == 404:
                        self._register()  # relay restarted and lost its mailbox
                        continue
                    response.raise_for_status()
                    break
                except Exception as exc:
                    Logger.warning(f"HttpSignaler: send failed: {exc}")
                    self._stop.wait(delay)
                    delay = min(delay * 2, 5.0)

    def _poll_loop(self) -> None:
        delay = 0.2
        while not self._stop.is_set():
            if not self._subscribed.wait(timeout=0.2):
                continue
            if self._stop.is_set():
                return
            try:
                response = self._request(
                    "GET", self._messages_url,
                    params={"after": self._cursor, "wait": self._poll_wait},
                    timeout=max(self._request_timeout, self._poll_wait + 5.0),
                )
                if self._stop.is_set():
                    return
                if response.status_code == 404:
                    self._register()
                    continue
                if response.status_code == 204:
                    delay = 0.2
                    continue
                response.raise_for_status()
                sequence = int(response.headers["X-Magpie-Sequence"])
                if sequence <= self._cursor:
                    continue
                if not self._subscribed.is_set() or self._stop.is_set():
                    continue  # leave cursor unchanged; resubscribe can replay
                callback = self._callback
                if callback is None:
                    continue
                try:
                    callback(response.content)
                except Exception as exc:
                    Logger.warning(f"HttpSignaler: callback error: {exc}")
                self._cursor = sequence
                delay = 0.2
            except Exception as exc:
                if self._stop.is_set():
                    return
                Logger.warning(f"HttpSignaler: poll failed: {exc}")
                self._stop.wait(delay)
                delay = min(delay * 2, 5.0)
