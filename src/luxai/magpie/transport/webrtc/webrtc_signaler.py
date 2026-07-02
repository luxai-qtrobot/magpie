"""
WebRTC signaling transport abstraction.

``WebRtcSignaler`` defines the minimal interface that a signaling transport
must implement: publish and subscribe raw bytes on a session-specific channel.

Built-in implementations
------------------------
* ``MqttSignaler``  — MQTT broker; needs ``paho-mqtt``
                      (``pip install luxai-magpie[mqtt]``).
* ``ZmqSignaler``   — ZMQ PAIR socket; needs ``pyzmq``
                      (bundled with the base magpie install).
"""

import queue
import threading
from abc import ABC, abstractmethod
from typing import Callable, Optional

from luxai.magpie.utils.logger import Logger


class WebRtcSignaler(ABC):
    """
    Abstract signaling transport for ``WebRTCConnection``.

    A signaler is responsible for **exchanging SDP and ICE candidate messages**
    between the two WebRTC peers.  The transport must be bidirectional: both
    peers must be able to publish to, and receive from, the shared channel.

    The signaling channel is fully internal to the signaler (derived from
    ``session_id``).  Callers only call :meth:`publish`, :meth:`subscribe`,
    :meth:`unsubscribe`, and :meth:`disconnect`.
    """

    @property
    @abstractmethod
    def session_id(self) -> str:
        """Shared session name used by both peers to find each other."""
        ...

    @abstractmethod
    def publish(self, payload: bytes) -> None:
        """Publish a raw signaling message to the shared channel."""
        ...

    @abstractmethod
    def subscribe(self, callback: Callable[[bytes], None]) -> None:
        """
        Register *callback* to be called with raw bytes whenever a signaling
        message arrives.  Only one callback is supported at a time.
        """
        ...

    @abstractmethod
    def unsubscribe(self) -> None:
        """Remove the previously registered callback."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Shut down the signaling transport and release resources."""
        ...


# ---------------------------------------------------------------------------
# MqttSignaler
# ---------------------------------------------------------------------------

class MqttSignaler(WebRtcSignaler):
    """
    MQTT-backed signaling transport.

    Both WebRTC peers subscribe and publish to the same MQTT topic::

        magpie/webrtc/<session_id>/signal

    This works over any IP network (including the internet) as long as both
    peers can reach the broker.

    Requires: ``pip install "luxai-magpie[mqtt]"``

    Example::

        signaler = MqttSignaler("mqtt://broker.hivemq.com:1883",
                                session_id="my-robot")
        conn = WebRTCConnection(signaler=signaler)
        conn.connect()
    """

    def __init__(
        self,
        broker_url: str,
        session_id: str,
        *,
        client_id: Optional[str] = None,
        timeout: float = 10.0,
        options=None,
        protocol_version: int = 5,
    ):
        """
        Args:
            broker_url: MQTT broker URI, e.g. ``mqtt://broker.hivemq.com:1883``.
            session_id: Shared rendezvous name — must be identical on both peers.
            client_id:  Optional MQTT client identifier.
            timeout:    Broker connection timeout in seconds (default: 10).
            options:    Optional ``MqttOptions`` for auth, TLS, reconnect, etc.

        Raises:
            ImportError:      If ``paho-mqtt`` is not installed.
            ConnectionError:  If the broker cannot be reached within *timeout*.
        """
        try:
            from luxai.magpie.transport.mqtt import MqttConnection  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "MqttSignaler requires paho-mqtt. "
                "Install with: pip install 'luxai-magpie[mqtt]'"
            )

        self._session_id = session_id
        self._topic = f"magpie/webrtc/{session_id}/signal"
        self._callback: Optional[Callable[[bytes], None]] = None

        self._conn = MqttConnection(broker_url, client_id=client_id, options=options, protocol_version=protocol_version)
        if not self._conn.connect(timeout=timeout):
            raise ConnectionError(
                f"MqttSignaler: could not connect to MQTT broker '{broker_url}'"
            )

    # ------------------------------------------------------------------ #
    # WebRtcSignaler interface                                             #
    # ------------------------------------------------------------------ #

    @property
    def session_id(self) -> str:
        return self._session_id

    def publish(self, payload: bytes) -> None:
        self._conn.publish(self._topic, payload)

    def subscribe(self, callback: Callable[[bytes], None]) -> None:
        self._callback = callback
        self._conn.add_subscription(self._topic, self._on_mqtt_message)

    def unsubscribe(self) -> None:
        self._conn.remove_subscription(self._topic, self._on_mqtt_message)
        self._callback = None

    def disconnect(self) -> None:
        self._conn.disconnect()

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _on_mqtt_message(self, payload_bytes: bytes, topic: str) -> None:
        cb = self._callback
        if cb is not None:
            cb(payload_bytes)


# ---------------------------------------------------------------------------
# ZmqSignaler
# ---------------------------------------------------------------------------

class ZmqSignaler(WebRtcSignaler):
    """
    ZMQ PAIR socket signaling transport — broker-less, LAN / local use.

    Uses a ZMQ ``PAIR`` socket which is inherently bidirectional.  One peer
    must bind (``bind=True``) and the other must connect (``bind=False``,
    the default).

    Requires: ``pyzmq`` (already a dependency of the base magpie install).

    Example::

        # Robot side — binds and listens:
        signaler = ZmqSignaler("tcp://*:5555", session_id="my-robot", bind=True)

        # Operator side — connects to the robot:
        signaler = ZmqSignaler("tcp://192.168.1.10:5555",
                               session_id="my-robot", bind=False)

        conn = WebRTCConnection(signaler=signaler)
        conn.connect()
    """

    def __init__(self, endpoint: str, session_id: str, *, bind: bool = False):
        """
        Args:
            endpoint:   ZMQ endpoint, e.g. ``tcp://192.168.1.10:5555``.
                        Use ``tcp://*:5555`` (or ``tcp://0.0.0.0:5555``)
                        when binding.
            session_id: Shared rendezvous name (used for logging only).
            bind:       ``True`` → bind the socket; ``False`` → connect (default).

        Raises:
            ImportError: If ``pyzmq`` is not installed.
        """
        try:
            import zmq  # noqa: F401
        except ImportError:
            raise ImportError(
                "ZmqSignaler requires pyzmq. "
                "Install with: pip install pyzmq"
            )

        self._session_id = session_id
        self._endpoint = endpoint
        self._bind = bind
        self._callback: Optional[Callable[[bytes], None]] = None
        self._closed = False
        self._send_queue: queue.Queue = queue.Queue()

        self._thread = threading.Thread(target=self._run, name="ZmqSignaler", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ #
    # WebRtcSignaler interface                                             #
    # ------------------------------------------------------------------ #

    @property
    def session_id(self) -> str:
        return self._session_id

    def publish(self, payload: bytes) -> None:
        if not self._closed:
            self._send_queue.put(payload)

    def subscribe(self, callback: Callable[[bytes], None]) -> None:
        self._callback = callback

    def unsubscribe(self) -> None:
        self._callback = None

    def disconnect(self) -> None:
        self._closed = True
        self._send_queue.put(None)  # wake the background thread
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    # ------------------------------------------------------------------ #
    # Internal: background I/O thread                                     #
    # ------------------------------------------------------------------ #

    def _run(self) -> None:
        import zmq

        ctx = zmq.Context()
        sock = ctx.socket(zmq.PAIR)
        sock.setsockopt(zmq.LINGER, 0)
        if self._bind:
            sock.bind(self._endpoint)
            Logger.debug(f"ZmqSignaler: bound at {self._endpoint}")
        else:
            sock.connect(self._endpoint)
            Logger.debug(f"ZmqSignaler: connected to {self._endpoint}")

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        try:
            while not self._closed:
                # Drain the outgoing queue
                while True:
                    try:
                        payload = self._send_queue.get_nowait()
                        if payload is None:  # shutdown sentinel
                            return
                        sock.send(payload)
                    except queue.Empty:
                        break
                    except zmq.ZMQError as e:
                        Logger.warning(f"ZmqSignaler: send error: {e}")
                        break

                # Wait up to 100 ms for incoming data
                try:
                    events = dict(poller.poll(100))
                except zmq.ZMQError:
                    break

                if sock in events:
                    try:
                        data = sock.recv()
                        cb = self._callback
                        if cb is not None:
                            try:
                                cb(data)
                            except Exception as e:
                                Logger.warning(f"ZmqSignaler: callback error: {e}")
                    except zmq.ZMQError as e:
                        Logger.warning(f"ZmqSignaler: recv error: {e}")
        finally:
            sock.close(linger=0)
            try:
                ctx.term()
            except Exception:
                pass
