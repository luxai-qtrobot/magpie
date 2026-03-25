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

from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import (
    WebRTCConnection, WebRTCRpcRequester, WebRTCOptions
)
from luxai.magpie.utils import Logger


BROKER_URI   = "mqtt://broker.hivemq.com:1883"
SESSION_ID   = "magpie/examples/webrtc-rpc"
SERVICE_NAME = "robot/motion"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-rpcreq")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    opts = WebRTCOptions(
        session_id=SESSION_ID,
        stun_servers=["stun:stun.l.google.com:19302"],
    )
    conn = WebRTCConnection(signaling=signal_conn, options=opts)
    if not conn.connect(timeout=20.0):
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
    signal_conn.disconnect()
