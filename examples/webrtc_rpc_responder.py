"""
WebRTC RPC Responder example.

Handles RPC calls from the operator over a WebRTC data channel and echoes
them back with an "ok" status.

Usage (run together with webrtc_rpc_requester.py):
    Terminal 1 (robot):    python examples/webrtc_rpc_responder.py
    Terminal 2 (operator): python examples/webrtc_rpc_requester.py
"""

from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import (
    WebRTCConnection, WebRTCRpcResponder, WebRTCOptions
)
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"
SESSION_ID   = "magpie/examples/webrtc-rpc"
SERVICE_NAME = "robot/motion"


def on_request(request: object) -> object:
    Logger.info(f"on_request: {request}")
    return {"status": "ok", "echo": request}


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-rpcresp")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    opts = WebRTCOptions(
        session_id=SESSION_ID,
        stun_servers=["stun:stun.l.google.com:19302"],
    )
    conn = WebRTCConnection(signaling=signal_conn, options=opts)
    if not conn.connect(timeout=20.0):
        raise SystemExit("WebRTC handshake timed out.")

    responder = WebRTCRpcResponder(conn, service_name=SERVICE_NAME)

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
    signal_conn.disconnect()
