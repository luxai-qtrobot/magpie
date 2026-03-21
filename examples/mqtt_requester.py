"""
MQTT RPC Requester example.

Sends RPC requests to the free HiveMQ public test broker at 1 Hz.
No account or credentials required.

Usage (run together with mqtt_responder.py):
    Terminal 1:  python examples/mqtt_responder.py
    Terminal 2:  python examples/mqtt_requester.py
"""

import time

from luxai.magpie.transport import MqttConnection, MqttRpcRequester
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"
SERVICE_NAME = "magpie/examples/rpc"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = MqttConnection(BROKER_URI, client_id="magpie-requester-example")
    if not conn.connect(timeout=10.0):
        Logger.error("Could not connect to broker — check your network connection.")
        raise SystemExit(1)

    client = MqttRpcRequester(conn, service_name=SERVICE_NAME)

    count = 1
    while True:
        try:
            response = client.call({"count": count, "action": "greet"}, timeout=5.0)
            Logger.info(f"call #{count} response: {response}")
            count += 1
            time.sleep(1)
        except TimeoutError:
            Logger.warning("RPC call timed out — is mqtt_responder.py running?")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    client.close()
    conn.disconnect()
