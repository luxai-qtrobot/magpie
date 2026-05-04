"""
WebRTC RPC Responder example.

Handles RPC calls from the operator over a WebRTC data channel and echoes
them back with an "ok" status.

Usage (run together with webrtc_rpc_requester.py):
    Terminal 1 (robot):    python examples/webrtc_rpc_responder.py
    Terminal 2 (operator): python examples/webrtc_rpc_requester.py
"""

from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcResponder
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"  # MQTT broker used only for signaling
SESSION_ID   = "magpie/examples/webrtc-rpc"     # shared rendezvous name — must match requester
SERVICE_NAME = "robot/motion"                    # RPC service name to expose


def on_request(request: object) -> object:
    Logger.info(f"on_request: {request}")
    return {"status": "ok", "echo": request}


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # For broker-less LAN use with_zmq() instead:
    conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", SESSION_ID, bind=True)
    # conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID, client_id="magpie-webrtc-rpcresp")
    if not conn.connect():
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
