from typing import Optional

from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger
from .mqtt_connection import MqttConnection


class MqttPublisher(StreamWriter):
    """
    MQTT-based stream publisher.

    Serializes messages and publishes them to the broker through a shared
    ``MqttConnection``.  The *topic* argument of ``write()`` becomes the
    MQTT topic — standard MQTT topic conventions apply (``/`` separators,
    no wildcards on publish).

    Usage::

        conn = MqttConnection("mqtt://broker.example.com:1883")
        conn.connect()

        pub = MqttPublisher(conn)
        pub.write({"sensor": "temp", "value": 22.5}, topic="sensors/temperature")

        pub.close()
        conn.disconnect()
    """

    def __init__(
        self,
        connection: MqttConnection,
        serializer=None,
        queue_size: int = 10,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
    ):
        """
        Args:
            connection: Shared ``MqttConnection`` instance (already connected or not yet
                        connected — messages are buffered in the writer queue).
            serializer: Serializer for outgoing messages. Defaults to ``MsgpackSerializer``.
            queue_size: Size of the internal write-ahead queue. Oldest messages are
                        dropped when full (latest-value semantics).
            qos: QoS level override (0, 1, or 2). Falls back to
                 ``connection.options.defaults.publish_qos``.
            retain: Retain flag override. Falls back to
                    ``connection.options.defaults.publish_retain``.
        """
        self._connection = connection
        self._serializer = serializer or MsgpackSerializer()
        self._qos = qos
        self._retain = retain

        super().__init__(name="MqttPublisher", queue_size=queue_size)
        Logger.debug(f"MqttPublisher: ready (broker={connection.uri})")

    # ------------------------------------------------------------------
    # StreamWriter implementation
    # ------------------------------------------------------------------

    def _transport_write(self, data: object, topic: str):
        if not topic:
            Logger.warning("MqttPublisher: write() called without a topic — message dropped.")
            return
        try:
            payload = self._serializer.serialize(data)
            self._connection.publish(topic, payload, qos=self._qos, retain=self._retain)
        except Exception as e:
            Logger.warning(f"MqttPublisher: write failed on topic '{topic}': {e}")

    def _transport_close(self):
        # The connection is shared; closing the publisher does NOT disconnect.
        Logger.debug("MqttPublisher: closed.")
