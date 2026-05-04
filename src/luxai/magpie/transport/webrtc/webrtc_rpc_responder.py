from queue import Queue, Empty
from typing import Optional

from luxai.magpie.transport.rpc_responder import RpcResponder
from luxai.magpie.utils.logger import Logger
from .webrtc_connection import WebRTCConnection


class WebRTCRpcResponder(RpcResponder):
    """
    WebRTC-based RPC responder.

    Listens for RPC requests from the remote peer on the ``"magpie"`` data
    channel, sends an immediate ACK, invokes the user-supplied handler, and
    replies — all over the same bidirectional data channel.

    Because the channel is P2P, no ``reply_to`` topic is required; the ``rid``
    alone is sufficient to route the reply back to the correct requester.

    Protocol::

        Requester → data channel: {"type": "rpc_req", "service": "...",
                                    "rid": "<ulid>", "payload": <request>}
        Responder → data channel: {"type": "rpc_ack", "rid": "<ulid>"}
        Responder → data channel: {"type": "rpc_rep", "rid": "<ulid>",
                                    "payload": <response>}

    Usage::

        def on_request(request):
            return {"status": "ok", "echo": request}

        conn = WebRTCConnection.with_mqtt("mqtt://broker:1883", session_id="my-robot")
        conn.connect()

        responder = WebRTCRpcResponder(conn, service_name="robot/motion")
        while True:
            try:
                responder.handle_once(handler=on_request, timeout=1.0)
            except TimeoutError:
                pass
            except KeyboardInterrupt:
                responder.close()
                break
    """

    def __init__(
        self,
        connection: WebRTCConnection,
        service_name: str,
        name: Optional[str] = None,
        schema=None,
    ):
        """
        Args:
            connection:   Shared ``WebRTCConnection`` instance.
            service_name: Service identifier.  The requester must use the
                          same name.  A leading ``/`` is stripped.
            name:         Display name for logging.
        """
        self._connection = connection
        self._service_name = service_name.lstrip("/")

        # Incoming request queue: connection routing → queue → _transport_recv
        self._req_queue: Queue = Queue()

        self._connection.add_rpc_service(self._service_name, self._req_queue)

        super().__init__(name=name or "WebRTCRpcResponder", schema=schema)
        Logger.debug(f"{self.name}: listening on service '{self._service_name}'.")

    # ------------------------------------------------------------------
    # RpcResponder implementation
    # ------------------------------------------------------------------

    def _transport_recv(self, timeout: float = None) -> tuple:
        """
        Block until a request arrives or *timeout* seconds elapse.

        Returns:
            (request_payload, client_ctx) where ``client_ctx`` carries the
            ``rid`` needed by ``_transport_send``.

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

        # Validate envelope
        if (
            not isinstance(msg, dict)
            or "rid" not in msg
            or "payload" not in msg
        ):
            raise RuntimeError(f"{self.name}: malformed request: {msg}")

        rid = msg["rid"]

        # Send ACK immediately before invoking the handler
        try:
            self._connection.send_data({"type": "rpc_ack", "rid": rid})
        except Exception as e:
            Logger.warning(f"{self.name}: ACK send error for rid='{rid}': {e}")

        return msg["payload"], {"rid": rid}

    def _transport_send(self, response_obj: object, client_ctx: object) -> None:
        """
        Send the handler's response back to the requester.

        Args:
            response_obj: The value returned by the user-supplied handler.
            client_ctx:   The dict from ``_transport_recv`` containing ``rid``.
        """
        try:
            self._connection.send_data({
                "type":    "rpc_rep",
                "rid":     client_ctx["rid"],
                "payload": response_obj,
            })
        except Exception as e:
            Logger.warning(f"{self.name}: response send error: {e}")
            raise

    def _transport_close(self) -> None:
        self._connection.remove_rpc_service(self._service_name)
        Logger.debug(f"{self.name}: closed.")
