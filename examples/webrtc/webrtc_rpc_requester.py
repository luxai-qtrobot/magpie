"""
WebRTC RPC Requester example.

Sends RPC calls directly to the robot over a WebRTC data channel — no
broker in the hot path, lower latency than MQTT RPC.  MQTT is used only
for the initial signaling handshake.

Usage (run together with webrtc_rpc_responder.py):
    Terminal 1 (robot):    python examples/webrtc_rpc_responder.py
    Terminal 2 (operator): python examples/webrtc_rpc_requester.py
"""

import time

from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcRequester
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"  # MQTT broker used only for signaling
SESSION_ID   = "magpie/examples/webrtc-rpc"     # shared rendezvous name — must match responder
SERVICE_NAME = "robot/motion"                    # RPC service to call


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # For broker-less LAN use with_zmq() instead:
    conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", SESSION_ID, bind=False)
    # conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID, client_id="magpie-webrtc-rpcreq")
    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    client = WebRTCRpcRequester(conn, service_name=SERVICE_NAME)

    count = 1
    while True:
        try:
            response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
            Logger.info(f"call #{count} response: {response}")
            count += 1
            time.sleep(1)
        except TimeoutError:
            Logger.warning("RPC timed out — is webrtc_rpc_responder.py running?")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    client.close()
    conn.disconnect()
