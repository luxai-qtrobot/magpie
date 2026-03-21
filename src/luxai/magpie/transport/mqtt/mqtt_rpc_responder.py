from queue import Queue, Empty
from typing import Optional

from luxai.magpie.transport.rpc_responder import RpcResponder
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger
from .mqtt_connection import MqttConnection


class MqttRpcResponder(RpcResponder):
    """
    MQTT-based RPC responder.

    Listens on ``<service_name>/rpc/req`` for incoming requests and replies
    to the per-request ``reply_to`` topic following this protocol:

    1. Receives request:
       ``{"rid": "<ulid>", "reply_to": "<reply_topic>", "payload": <request>}``

    2. Immediately sends ACK to ``reply_to``:
       ``{"rid": "<ulid>", "ack": true}``

    3. Calls the user-supplied handler with the deserialized request payload.

    4. Sends the handler's return value to ``reply_to``:
       ``{"rid": "<ulid>", "payload": <response>}``

    Usage::

        conn = MqttConnection("mqtt://broker.example.com:1883")
        conn.connect()

        def handler(request):
            print("Got:", request)
            return {"status": "ok", "echo": request}

        responder = MqttRpcResponder(conn, service_name="myrobot/motion")
        while True:
            try:
                responder.handle_once(handler=handler, timeout=1.0)
            except TimeoutError:
                pass
            except KeyboardInterrupt:
                responder.close()
                break

        conn.disconnect()
    """

    def __init__(
        self,
        connection: MqttConnection,
        service_name: str,
        serializer=None,
        name: Optional[str] = None,
        qos: Optional[int] = None,
    ):
        """
        Args:
            connection: Shared ``MqttConnection`` instance.
            service_name: Service identifier. Derives the request topic:
                          ``<service_name>/rpc/req``. Leading ``/`` is stripped.
            serializer: Serializer for messages. Defaults to ``MsgpackSerializer``.
            name: Display name for logging. Defaults to ``MqttRpcResponder``.
            qos: QoS override for both subscribe and publish. Falls back to
                 the connection defaults (publish_qos / subscribe_qos).
        """
        self._connection = connection
        self._serializer = serializer or MsgpackSerializer()
        self._qos = qos

        svc = service_name.lstrip("/")
        self._req_topic = f"{svc}/rpc/req"

        # Incoming request queue: MQTT callback → queue → _transport_recv
        self._req_queue: Queue = Queue()

        self._connection.add_subscription(self._req_topic, self._on_request, qos=self._qos)

        super().__init__(name=name or "MqttRpcResponder")
        Logger.debug(f"{self.name}: listening on '{self._req_topic}'")

    # ------------------------------------------------------------------
    # Internal: request callback
    # ------------------------------------------------------------------

    def _on_request(self, payload_bytes: bytes, topic: str):
        """Dispatched by MqttConnection for every message on the request topic."""
        try:
            msg = self._serializer.deserialize(payload_bytes)
            self._req_queue.put_nowait(msg)
        except Exception as e:
            Logger.warning(f"{self.name}: failed to deserialize request: {e}")

    # ------------------------------------------------------------------
    # RpcResponder implementation
    # ------------------------------------------------------------------

    def _transport_recv(self, timeout: float = None) -> tuple:
        """
        Block until a request arrives or *timeout* seconds elapse.

        Returns:
            (request_payload, client_ctx) where ``client_ctx`` carries the
            ``rid`` and ``reply_to`` topic needed by ``_transport_send``.

        Raises:
            TimeoutError: If no request arrives within *timeout*.
            RuntimeError: If the request envelope is malformed.
        """
        try:
            msg = self._req_queue.get(timeout=timeout)
        except Empty:
            raise TimeoutError(
                f"{self.name}: no request received"
                + (f" within {timeout}s" if timeout is not None else "")
            )

        # Validate the request envelope
        if (
            not isinstance(msg, dict)
            or "rid" not in msg
            or "payload" not in msg
            or "reply_to" not in msg
        ):
            raise RuntimeError(f"{self.name}: malformed request: {msg}")

        rid = msg["rid"]
        reply_to = msg["reply_to"]

        # Send ACK immediately before invoking the handler
        try:
            ack = self._serializer.serialize({"rid": rid, "ack": True})
            self._connection.publish(
                reply_to,
                ack,
                qos=self._qos if self._qos is not None else 1,
            )
        except Exception as e:
            Logger.warning(f"{self.name}: ACK send error for rid='{rid}': {e}")

        client_ctx = {"rid": rid, "reply_to": reply_to}
        return msg["payload"], client_ctx

    def _transport_send(self, response_obj: object, client_ctx: object):
        """
        Serialize and publish the handler's response to the requester's reply topic.

        Args:
            response_obj: The value returned by the user-supplied handler.
            client_ctx: The dict returned by ``_transport_recv`` containing
                        ``rid`` and ``reply_to``.
        """
        try:
            resp = self._serializer.serialize({
                "rid": client_ctx["rid"],
                "payload": response_obj,
            })
            self._connection.publish(
                client_ctx["reply_to"],
                resp,
                qos=self._qos if self._qos is not None else 1,
            )
        except Exception as e:
            Logger.warning(f"{self.name}: response send error: {e}")
            raise

    def _transport_close(self):
        self._connection.remove_subscription(self._req_topic, self._on_request)
        Logger.debug(f"{self.name}: closed.")
