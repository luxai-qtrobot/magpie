import time
import threading
from dataclasses import dataclass
from luxai.magpie.utils.logger import Logger
from luxai.magpie.transport.rpc_requester import RpcRequester, AckTimeoutError, ReplyTimeoutError
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.common import get_uinque_id
from .zmq_utils import zmq


@dataclass
class _PendingCall:
    ack_event: threading.Event
    reply_event: threading.Event
    reply_payload: object = None
    reply_error: Exception = None


class ZMQRpcRequester(RpcRequester):
    """
    ZMQRpcRequester class.

    This class represents an RPC client using a ZeroMQ DEALER socket.
    It serializes request objects, sends them to the ROUTER peer, and
    deserializes responses using the provided serializer.
    """

    def __init__(
        self,
        endpoint: str,
        serializer=None,
        name: str = None,
        identity: bytes = None,
        ack_timeout: float = 2.0,
        schema=None,
    ):
        """
        Initializes the ZMQRpcRequester.
        """
        self.endpoint = endpoint
        self.serializer = serializer or MsgpackSerializer()
        self.ack_timeout = ack_timeout

        # Use shared context for inproc, otherwise create a new one
        self.context = zmq.Context.instance() if endpoint.startswith("inproc:") else zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)

        if identity is not None:
            self.socket.setsockopt(zmq.IDENTITY, identity)

        self.socket.connect(endpoint)

        # ----------------------------
        # Demux + single I/O thread
        # ----------------------------
        self._pending_lock = threading.Lock()
        self._pending: dict[str, _PendingCall] = {}

        self._closing = False
        self._io_thread: threading.Thread | None = None

        # Inproc control channel (callers -> I/O thread) so we can poll both sockets.
        # NOTE: callers never touch DEALER; only I/O thread owns it.
        self._ctrl_endpoint = f"inproc://zmq-rpc-req-{get_uinque_id()}"
        self._ctrl_pull = self.context.socket(zmq.PULL)
        self._ctrl_pull.bind(self._ctrl_endpoint)

        # Thread-local PUSH socket so we don't share sockets between threads.
        self._tls = threading.local()

        # preload name so that Logger calls in IO thread have correct name
        self.name = name if name is not None else "ZMQRpcRequester"
        self._start_io_thread()

        super().__init__(name=self.name, schema=schema)
        Logger.debug(f"{self.name} connected to {self.endpoint} as DEALER.")

    # --------------------------------------------------
    # Internal: IO thread
    # --------------------------------------------------
    def _start_io_thread(self) -> None:
        self._io_thread = threading.Thread(target=self._io_loop, name=f"{self.name}-io", daemon=True)
        self._io_thread.start()

    def _get_ctrl_push(self):
        s = getattr(self._tls, "ctrl_push", None)
        if s is None or s.closed:
            s = self.context.socket(zmq.PUSH)
            s.connect(self._ctrl_endpoint)
            self._tls.ctrl_push = s
        return s

    def _io_loop(self) -> None:
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        poller.register(self._ctrl_pull, zmq.POLLIN)

        Logger.debug(f"{self.name}: I/O loop started.")

        while True:
            # Block until either:
            # - a reply arrives on DEALER
            # - a new outgoing request arrives on control inproc socket
            # - a close sentinel arrives
            try:
                events = dict(poller.poll())
            except zmq.ZMQError as e:
                if self._closing:
                    break
                Logger.warning(f"{self.name}: I/O poll error: {e}")
                break

            # ---- Outgoing control messages ----
            if self._ctrl_pull in events and (events[self._ctrl_pull] & zmq.POLLIN):
                try:
                    ctrl = self._ctrl_pull.recv()
                except Exception as e:
                    if self._closing:
                        break
                    Logger.warning(f"{self.name}: control recv error: {e}")
                    continue

                # Close sentinel
                if ctrl == b"__CLOSE__":
                    break

                # Otherwise ctrl is a serialized request bytes for DEALER
                try:
                    self.socket.send(ctrl)
                except Exception as e:
                    Logger.warning(f"{self.name}: transport error during send in I/O loop: {e}")
                    # Fail all pending (send is broken)
                    self._fail_all_pending(RuntimeError(f"{self.name}: transport send failure: {e}"))
                    break

            # ---- Incoming replies (ACK or final reply) ----
            if self.socket in events and (events[self.socket] & zmq.POLLIN):
                try:
                    reply_bytes = self.socket.recv()
                    msg = self.serializer.deserialize(reply_bytes)
                except Exception as e:
                    if self._closing:
                        break
                    Logger.warning(f"{self.name}: transport error during recv in I/O loop: {e}")
                    continue

                rid = None
                try:
                    rid = msg.get("rid")
                except Exception:
                    rid = None

                if not rid:
                    Logger.warning(f"{self.name}: received message without rid: {msg}")
                    continue

                with self._pending_lock:
                    pending = self._pending.get(rid)

                if pending is None:
                    # late/unknown message
                    Logger.debug(f"{self.name}: received message for unknown rid={rid}: {msg}")
                    continue

                # ACK message
                if msg.get("ack", False):
                    pending.ack_event.set()
                    continue

                # Reply message
                if "payload" in msg:
                    pending.reply_payload = msg["payload"]
                    pending.reply_event.set()
                    continue

                # Anything else: treat as invalid reply
                pending.reply_error = RuntimeError(f"{self.name}: invalid reply received: {msg}")
                pending.reply_event.set()

        Logger.debug(f"{self.name}: I/O loop exiting.")
        # Ensure callers don't block forever on shutdown
        self._fail_all_pending(RuntimeError(f"{self.name}: transport closed"))

    def _fail_all_pending(self, err: Exception) -> None:
        with self._pending_lock:
            items = list(self._pending.items())
            self._pending.clear()

        for _, p in items:
            p.reply_error = err
            p.ack_event.set()
            p.reply_event.set()

    # --------------------------------------------------
    # RpcRequester: transport call
    # --------------------------------------------------
    def _transport_call(self, request_obj: object, timeout: float = None) -> object:
        """
        Performs the transport-level RPC call via ZeroMQ DEALER.
        """
        if self._closing:
            raise RuntimeError(f"{self.name}: transport is closed")

        # ---- Send request ----
        rid = get_uinque_id()
        try:
            req = {
                "rid": rid,
                "payload": request_obj,
            }
            payload = self.serializer.serialize(req)

            # Register pending BEFORE send (so we can catch very fast ACK/reply).
            pending = _PendingCall(ack_event=threading.Event(), reply_event=threading.Event())
            with self._pending_lock:
                self._pending[rid] = pending

            # Send to I/O thread (do not touch DEALER from caller threads)
            self._get_ctrl_push().send(payload)

        except Exception as e:
            Logger.warning(f"{self.name}: transport error during RPC call: {e}")
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise

        # ---- Wait for ACK ----
        ack_timeout = min(timeout, self.ack_timeout) if timeout else self.ack_timeout
        if not pending.ack_event.wait(timeout=ack_timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise AckTimeoutError(f"{self.name}: no ack received within {ack_timeout} seconds")

        # If transport died while we were waiting for ACK
        if pending.reply_error is not None and not pending.reply_event.is_set():
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise pending.reply_error

        # ---- Wait for reply ----
        if not pending.reply_event.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise ReplyTimeoutError(f"{self.name}: no reply received within {timeout} seconds")

        with self._pending_lock:
            self._pending.pop(rid, None)

        if pending.reply_error is not None:
            raise pending.reply_error

        return pending.reply_payload


    def _transport_close(self) -> None:
        """
        Closes the ZeroMQ socket and performs any necessary cleanup.
        """
        Logger.debug(f"{self.name} is closing ZMQ DEALER socket.")
        self._closing = True

        # Wake the I/O loop so it can exit (no polling timeouts needed)
        try:
            tmp = self.context.socket(zmq.PUSH)
            tmp.connect(self._ctrl_endpoint)
            tmp.send(b"__CLOSE__")
            tmp.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: close signal error: {e}")

        # Join I/O thread
        try:
            if self._io_thread is not None:
                self._io_thread.join(timeout=2.0)
        except Exception as e:
            Logger.warning(f"{self.name}: I/O thread join error: {e}")

        # Fail any remaining pending calls (just in case)
        self._fail_all_pending(RuntimeError(f"{self.name}: transport closed"))

        # Close sockets immediately without waiting for peer
        try:
            self.socket.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: socket close error: {e}")

        try:
            self._ctrl_pull.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: ctrl socket close error: {e}")

        # Close thread-local control PUSH socket if created in this thread
        try:
            s = getattr(self._tls, "ctrl_push", None)
            if s is not None and not s.closed:
                s.close(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: ctrl push close error: {e}")

        # Terminate context if we created it.
        # Use destroy(linger=0) instead of term() so that sockets created in
        # thread-pool threads (e.g. by anyio.to_thread.run_sync) are forcibly
        # closed and don't block context termination.
        try:
            if not self.endpoint.startswith("inproc:"):
                self.context.destroy(linger=0)
        except Exception as e:
            Logger.warning(f"{self.name}: context close error: {e}")
