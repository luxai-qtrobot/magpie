"""
WebRTC Publisher example.

Streams sensor data from robot to operator over the internet using a WebRTC
data channel.  MQTT is used only for the initial signaling handshake (SDP +
ICE exchange); all payload traffic flows P2P via WebRTC.

Usage (run together with webrtc_subscriber.py):
    Terminal 1 (robot):    python examples/webrtc_publisher.py
    Terminal 2 (operator): python examples/webrtc_subscriber.py

TIP: Change SESSION_ID to something unique to avoid collisions with other
     users sharing the same public broker.
"""

import time

from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"
SESSION_ID = "magpie/examples/webrtc"
TOPIC      = "robot/state"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # Signaling transport — MQTT over internet (or ZMQ for LAN)
    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-pub")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    # WebRTC connection — role and ICE negotiation are automatic
    from luxai.magpie.transport.webrtc import WebRTCOptions
    opts = WebRTCOptions(
        session_id=SESSION_ID,
        stun_servers=["stun:stun.l.google.com:19302"],
        # turn_servers=[WebRTCTurnServer(url="turn:myturn.server:3478",
        #                                username="u", credential="p")],
    )
    conn = WebRTCConnection(signaling=signal_conn, options=opts)
    if not conn.connect(timeout=20.0):
        raise SystemExit("WebRTC handshake timed out.")

    pub = WebRTCPublisher(conn)

    count = 1
    while True:
        try:
            pub.write({"count": count, "motor": [0.1, 0.2, 0.3]}, topic=TOPIC)
            Logger.info(f"published #{count} to '{TOPIC}'")
            count += 1
            time.sleep(0.1)
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    pub.close()
    conn.disconnect()
    signal_conn.disconnect()
