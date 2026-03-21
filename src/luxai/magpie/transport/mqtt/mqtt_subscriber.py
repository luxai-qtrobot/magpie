from queue import Queue, Empty
from typing import Optional, Tuple

from luxai.magpie.transport.stream_reader import StreamReader
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger
from .mqtt_connection import MqttConnection


class MqttSubscriber(StreamReader):
    """
    MQTT-based stream subscriber.

    Subscribes to one MQTT topic (or wildcard pattern) through a shared
    ``MqttConnection`` and delivers deserialized messages via ``read()``.

    MQTT wildcard patterns are supported:
        - ``+`` matches a single topic level (e.g. ``sensors/+/temperature``)
        - ``#`` matches all remaining levels  (e.g. ``sensors/#``)

    Multiple ``MqttSubscriber`` instances can share the same connection and
    subscribe to the same or different topics without opening additional
    broker connections.

    Usage::

        conn = MqttConnection("mqtt://broker.example.com:1883")
        conn.connect()

        sub = MqttSubscriber(conn, topic="sensors/temperature")
        while True:
            try:
                data, topic = sub.read(timeout=5.0)
                print(topic, data)
            except TimeoutError:
                pass
            except KeyboardInterrupt:
                break

        sub.close()
        conn.disconnect()
    """

    def __init__(
        self,
        connection: MqttConnection,
        topic: str,
        serializer=None,
        queue_size: int = 10,
        qos: Optional[int] = None,
    ):
        """
        Args:
            connection: Shared ``MqttConnection`` instance.
            topic: MQTT topic or wildcard pattern to subscribe to.
            serializer: Deserializer for incoming messages. Defaults to ``MsgpackSerializer``.
            queue_size: Size of the internal reader queue.  The oldest message is
                        dropped when the queue is full (latest-value semantics).
            qos: QoS level override. Falls back to
                 ``connection.options.defaults.subscribe_qos``.
        """
        self._connection = connection
        self._topic = topic
        self._serializer = serializer or MsgpackSerializer()
        self._qos = qos

        # Unbounded internal queue: MQTT callbacks push here; the StreamReader
        # background thread drains it via _transport_read_blocking.
        # StreamReader's bounded reader_queue provides the actual back-pressure.
        self._msg_queue: Queue = Queue()

        # Register with the shared connection *before* starting the StreamReader
        # background thread so no messages are lost.
        self._connection.add_subscription(self._topic, self._on_message, qos=self._qos)

        super().__init__(name="MqttSubscriber", queue_size=queue_size)
        Logger.debug(
            f"MqttSubscriber: subscribed to '{topic}' (broker={connection.uri})"
        )

    # ------------------------------------------------------------------
    # Internal: MQTT message callback
    # ------------------------------------------------------------------

    def _on_message(self, payload_bytes: bytes, topic: str):
        """Called by MqttConnection._on_message for every matching message."""
        try:
            data = self._serializer.deserialize(payload_bytes)
            self._msg_queue.put_nowait((data, topic))
        except Exception as e:
            Logger.warning(
                f"MqttSubscriber: deserialization error for topic '{topic}': {e}"
            )

    # ------------------------------------------------------------------
    # StreamReader implementation
    # ------------------------------------------------------------------

    def _transport_read_blocking(self, timeout: float = None) -> Tuple[object, str]:
        """
        Block until a message is available or *timeout* seconds elapse.

        When called from the StreamReader background thread, *timeout* is
        always 1.0 s (the poll interval), so the thread checks the close
        event frequently.  When called directly (``queue_size=0``), *timeout*
        is whatever the user passed to ``read()``.
        """
        try:
            return self._msg_queue.get(timeout=timeout)
        except Empty:
            raise TimeoutError(
                f"MqttSubscriber: no data received"
                + (f" within {timeout}s" if timeout is not None else "")
            )

    def _transport_close(self):
        self._connection.remove_subscription(self._topic, self._on_message)
        Logger.debug(f"MqttSubscriber: unsubscribed from '{self._topic}'.")
