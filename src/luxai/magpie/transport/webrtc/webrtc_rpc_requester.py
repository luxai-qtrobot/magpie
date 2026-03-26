import threading
from dataclasses import dataclass
from typing import Optional

from luxai.magpie.transport.rpc_requester import RpcRequester, AckTimeoutError, ReplyTimeoutError
from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from .webrtc_connection import WebRTCConnection


@dataclass
class _PendingCall:
    """Tracks the state of one in-flight RPC call."""
    ack_event: threading.Event
    reply_event: threading.Event
    reply_payload: object = None
    reply_error: Exception = None


class WebRTCRpcRequester(RpcRequester):
    """
    WebRTC-based RPC requester.

    Sends RPC requests to the remote peer over the ``"magpie"`` data channel
    and waits for an ACK then a final reply.

    The protocol is identical to ``MqttRpcRequester`` but simpler: because
    the data channel is a bidirectional P2P pipe, no ``reply_to`` topic is
    needed — both the request and the reply travel on the same channel,
    demuxed by ``rid``.

    Protocol::

        Requester → data channel: {"type": "rpc_req", "service": "...",
                                    "rid": "<ulid>", "payload": <request>}
        Responder → data channel: {"type": "rpc_ack", "rid": "<ulid>"}
        Responder → data channel: {"type": "rpc_rep", "rid": "<ulid>",
                                    "payload": <response>}

    Usage::

        conn = WebRTCConnection.with_mqtt("mqtt://broker:1883", session_id="my-robot")
        conn.connect()

        client = WebRTCRpcRequester(conn, service_name="robot/motion")
        try:
            response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
        except TimeoutError:
            print("timed out")
        finally:
            client.close()
    """

    def __init__(
        self,
        connection: WebRTCConnection,
        service_name: str,
        name: Optional[str] = None,
        ack_timeout: float = 2.0,
    ):
        """
        Args:
            connection:   Shared ``WebRTCConnection`` instance.
            service_name: Service identifier matching the responder's
                          ``service_name``.
            name:         Display name for logging.
            ack_timeout:  Maximum seconds to wait for the ACK (default 2.0).
        """
        self._connection = connection
        self._service_name = service_name.lstrip("/")
        self.ack_timeout = ack_timeout

        self._pending_lock = threading.Lock()
        self._pending: dict[str, _PendingCall] = {}

        # Reply callbacks are registered per-rid in _transport_call
        super().__init__(name=name or "WebRTCRpcRequester")
        Logger.debug(
            f"{self.name}: ready for service '{self._service_name}'."
        )

    # ------------------------------------------------------------------
    # RpcRequester implementation
    # ------------------------------------------------------------------

    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        rid = get_uinque_id()

        pending = _PendingCall(
            ack_event=threading.Event(),
            reply_event=threading.Event(),
        )

        # Register before sending so a very fast reply is never missed
        with self._pending_lock:
            self._pending[rid] = pending

        # Register reply callback with the connection for this rid
        self._connection.register_rpc_reply(rid, self._on_reply)

        try:
            self._connection.send_data({
                "type":    "rpc_req",
                "service": self._service_name,
                "rid":     rid,
                "payload": request_obj,
            })
        except Exception as e:
            with self._pending_lock:
                self._pending.pop(rid, None)
            self._connection.unregister_rpc_reply(rid)
            raise

        # ---- Wait for ACK ----
        ack_timeout = min(timeout, self.ack_timeout) if timeout is not None else self.ack_timeout
        if not pending.ack_event.wait(timeout=ack_timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            self._connection.unregister_rpc_reply(rid)
            raise AckTimeoutError(
                f"{self.name}: no ACK from '{self._service_name}' "
                f"within {ack_timeout}s"
            )

        if pending.reply_error is not None and not pending.reply_event.is_set():
            with self._pending_lock:
                self._pending.pop(rid, None)
            self._connection.unregister_rpc_reply(rid)
            raise pending.reply_error

        # ---- Wait for reply ----
        if not pending.reply_event.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            self._connection.unregister_rpc_reply(rid)
            raise ReplyTimeoutError(
                f"{self.name}: no reply from '{self._service_name}' "
                f"within {timeout}s"
            )

        with self._pending_lock:
            self._pending.pop(rid, None)
        self._connection.unregister_rpc_reply(rid)

        if pending.reply_error is not None:
            raise pending.reply_error

        return pending.reply_payload

    def _transport_close(self) -> None:
        self._fail_all_pending(RuntimeError(f"{self.name}: requester closed"))
        Logger.debug(f"{self.name}: closed.")

    # ------------------------------------------------------------------
    # Internal: reply callback (called from WebRTCConnection routing)
    # ------------------------------------------------------------------

    def _on_reply(self, msg: dict) -> None:
        """Dispatched by WebRTCConnection for rpc_ack / rpc_rep messages."""
        rid = msg.get("rid")
        if not rid:
            return

        with self._pending_lock:
            pending = self._pending.get(rid)

        if pending is None:
            Logger.debug(f"{self.name}: late or unknown rid='{rid}'")
            return

        if msg.get("type") == "rpc_ack":
            pending.ack_event.set()
        elif msg.get("type") == "rpc_rep":
            if "payload" in msg:
                pending.reply_payload = msg["payload"]
            else:
                pending.reply_error = RuntimeError(
                    f"{self.name}: malformed reply for rid='{rid}': {msg}"
                )
            pending.reply_event.set()

    def _fail_all_pending(self, err: Exception) -> None:
        with self._pending_lock:
            items = list(self._pending.items())
            self._pending.clear()
        for rid, p in items:
            self._connection.unregister_rpc_reply(rid)
            p.reply_error = err
            p.ack_event.set()
            p.reply_event.set()
