import threading
from dataclasses import dataclass
from typing import Dict, Optional

from luxai.magpie.transport.rpc_requester import RpcRequester, AckTimeoutError, ReplyTimeoutError
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from .mqtt_connection import MqttConnection


@dataclass
class _PendingCall:
    """Tracks the state of one in-flight RPC call."""
    ack_event: threading.Event
    reply_event: threading.Event
    reply_payload: object = None
    reply_error: Exception = None


class MqttRpcRequester(RpcRequester):
    """
    MQTT-based RPC requester.

    Implements request/reply over MQTT topics following this protocol:

    1. Requester publishes to  ``<service_name>/rpc/req``:
       ``{"rid": "<ulid>", "reply_to": "<reply_topic>", "payload": <request>}``

    2. Responder sends ACK to ``<reply_topic>``:
       ``{"rid": "<ulid>", "ack": true}``

    3. Responder sends final reply to ``<reply_topic>``:
       ``{"rid": "<ulid>", "payload": <response>}``

    Each requester instance subscribes to its own unique reply topic so that
    multiple requesters sharing the same ``MqttConnection`` never mix up replies.

    Usage::

        conn = MqttConnection("mqtt://broker.example.com:1883")
        conn.connect()

        req = MqttRpcRequester(conn, service_name="myrobot/motion")
        try:
            response = req.call({"action": "move", "x": 1.0}, timeout=5.0)
            print("Response:", response)
        except TimeoutError:
            print("Timed out")
        finally:
            req.close()
            conn.disconnect()
    """

    def __init__(
        self,
        connection: MqttConnection,
        service_name: str,
        serializer=None,
        name: Optional[str] = None,
        ack_timeout: float = 2.0,
        qos: Optional[int] = None,
    ):
        """
        Args:
            connection: Shared ``MqttConnection`` instance.
            service_name: Service identifier.  Used to derive the request topic:
                          ``<service_name>/rpc/req``.  Leading ``/`` is stripped.
            serializer: Serializer for messages. Defaults to ``MsgpackSerializer``.
            name: Display name for logging. Defaults to ``MqttRpcRequester``.
            ack_timeout: Maximum seconds to wait for the ACK from the responder
                         (default 2.0).  Should be less than the total call timeout.
            qos: QoS override for both publish and subscribe. Falls back to
                 the connection defaults.
        """
        self._connection = connection
        self._serializer = serializer or MsgpackSerializer()
        self.ack_timeout = ack_timeout
        self._qos = qos

        # Derive topics
        svc = service_name.lstrip("/")
        self._req_topic = f"{svc}/rpc/req"

        # Unique reply topic per requester instance to avoid cross-talk when
        # multiple requesters share the same MqttConnection (same client_id).
        _instance_id = get_uinque_id()[:12]
        self._rep_topic = f"magpie/rpc/{connection.client_id}/{_instance_id}/rep"

        # Pending call registry
        self._pending_lock = threading.Lock()
        self._pending: Dict[str, _PendingCall] = {}

        # Subscribe to our private reply topic
        self._connection.add_subscription(self._rep_topic, self._on_reply, qos=self._qos)

        super().__init__(name=name or "MqttRpcRequester")
        Logger.debug(
            f"{self.name}: request topic='{self._req_topic}', "
            f"reply topic='{self._rep_topic}'"
        )

    # ------------------------------------------------------------------
    # Internal: reply callback
    # ------------------------------------------------------------------

    def _on_reply(self, payload_bytes: bytes, topic: str):
        """Dispatched by MqttConnection for every message on the reply topic."""
        try:
            msg = self._serializer.deserialize(payload_bytes)
        except Exception as e:
            Logger.warning(f"{self.name}: failed to deserialize reply: {e}")
            return

        rid = msg.get("rid") if isinstance(msg, dict) else None
        if not rid:
            Logger.warning(f"{self.name}: reply without 'rid' field: {msg}")
            return

        with self._pending_lock:
            pending = self._pending.get(rid)

        if pending is None:
            Logger.debug(f"{self.name}: late or unknown rid='{rid}'")
            return

        if msg.get("ack", False):
            pending.ack_event.set()
        elif "payload" in msg:
            pending.reply_payload = msg["payload"]
            pending.reply_event.set()
        else:
            pending.reply_error = RuntimeError(
                f"{self.name}: unexpected reply format: {msg}"
            )
            pending.reply_event.set()

    def _fail_all_pending(self, err: Exception):
        with self._pending_lock:
            items = list(self._pending.items())
            self._pending.clear()
        for _, p in items:
            p.reply_error = err
            p.ack_event.set()
            p.reply_event.set()

    # ------------------------------------------------------------------
    # RpcRequester implementation
    # ------------------------------------------------------------------

    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        """
        Publish the request and wait for ACK then final reply.

        Raises:
            AckTimeoutError: If the responder does not ACK within ``ack_timeout``.
            ReplyTimeoutError: If the responder does not reply within *timeout*.
        """
        rid = get_uinque_id()
        req = {
            "rid": rid,
            "reply_to": self._rep_topic,
            "payload": request_obj,
        }

        pending = _PendingCall(
            ack_event=threading.Event(),
            reply_event=threading.Event(),
        )

        # Register before publishing so a very fast reply is never missed.
        with self._pending_lock:
            self._pending[rid] = pending

        try:
            self._connection.publish(
                self._req_topic,
                self._serializer.serialize(req),
                qos=self._qos if self._qos is not None else 1,
            )
        except Exception as e:
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise

        # --- Wait for ACK ---
        ack_timeout = min(timeout, self.ack_timeout) if timeout is not None else self.ack_timeout
        if not pending.ack_event.wait(timeout=ack_timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise AckTimeoutError(
                f"{self.name}: no ACK from '{self._req_topic}' within {ack_timeout}s"
            )

        # Transport error set while waiting for ACK (e.g. connection closed)
        if pending.reply_error is not None and not pending.reply_event.is_set():
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise pending.reply_error

        # --- Wait for reply ---
        if not pending.reply_event.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise ReplyTimeoutError(
                f"{self.name}: no reply from '{self._req_topic}' within {timeout}s"
            )

        with self._pending_lock:
            self._pending.pop(rid, None)

        if pending.reply_error is not None:
            raise pending.reply_error

        return pending.reply_payload

    def _transport_close(self):
        self._connection.remove_subscription(self._rep_topic, self._on_reply)
        self._fail_all_pending(RuntimeError(f"{self.name}: requester closed"))
        Logger.debug(f"{self.name}: closed.")
