"""
MQTT RPC Responder example.

Listens for RPC requests on the free HiveMQ public test broker and echoes
them back with an "ok" status.  No account or credentials required.

Usage (run together with mqtt_requester.py):
    Terminal 1:  python examples/mqtt_responder.py
    Terminal 2:  python examples/mqtt_requester.py
"""

from luxai.magpie.transport import MqttConnection, MqttRpcResponder
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"
SERVICE_NAME = "magpie/examples"


def on_request(request: object) -> object:
    Logger.info(f"on_request: {request}")
    return {"status": "ok", "echo": request}


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = MqttConnection(BROKER_URI, client_id="magpie-responder-example")
    if not conn.connect(timeout=10.0):
        Logger.error("Could not connect to broker — check your network connection.")
        raise SystemExit(1)

    responder = MqttRpcResponder(conn, service_name=SERVICE_NAME)

    while True:
        try:
            responder.handle_once(handler=on_request, timeout=1.0)
        except TimeoutError:
            pass
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    responder.close()
    conn.disconnect()
