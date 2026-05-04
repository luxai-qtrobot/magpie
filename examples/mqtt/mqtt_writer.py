"""
MQTT Writer example.

Publishes a message to a topic at 1 Hz using the free HiveMQ public test broker.
No account or credentials required.

Usage (run together with mqtt_reader.py):
    Terminal 1:  python examples/mqtt_writer.py
    Terminal 2:  python examples/mqtt_reader.py

TIP: Change TOPIC to something unique to avoid collisions with other users
     sharing the same public broker.
"""

import time

from luxai.magpie.transport import MqttConnection, MqttStreamWriter
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"
TOPIC      = "magpie/examples/stream"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = MqttConnection(BROKER_URI, client_id="magpie-pub-example")
    if not conn.connect(timeout=10.0):
        Logger.error("Could not connect to broker — check your network connection.")
        raise SystemExit(1)

    pub = MqttStreamWriter(conn)

    count = 1
    while True:
        try:
            pub.write({"count": count, "msg": "hello from magpie-mqtt"}, topic=TOPIC)
            Logger.info(f"published #{count} to '{TOPIC}'")
            count += 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    pub.close()
    conn.disconnect()
