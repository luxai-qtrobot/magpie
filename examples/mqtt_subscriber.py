"""
MQTT Subscriber example.

Subscribes to a topic on the free HiveMQ public test broker.
No account or credentials required.

Usage (run together with mqtt_publisher.py):
    Terminal 1:  python examples/mqtt_publisher.py
    Terminal 2:  python examples/mqtt_subscriber.py

Wildcards are also supported:
    sub = MqttSubscriber(conn, topic="magpie/examples/+")   # single-level wildcard
    sub = MqttSubscriber(conn, topic="magpie/#")            # multi-level wildcard
"""

from luxai.magpie.transport import MqttConnection, MqttSubscriber
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"
TOPIC      = "magpie/examples/pubsub"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = MqttConnection(BROKER_URI, client_id="magpie-sub-example")
    if not conn.connect(timeout=10.0):
        Logger.error("Could not connect to broker — check your network connection.")
        raise SystemExit(1)

    sub = MqttSubscriber(conn, topic=TOPIC)

    while True:
        try:
            data, topic = sub.read(timeout=5.0)
            Logger.info(f"received on '{topic}': {data}")
        except TimeoutError:
            Logger.debug("waiting for messages...")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    sub.close()
    conn.disconnect()
