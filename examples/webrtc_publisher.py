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

from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher, WebRTCOptions
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"  # MQTT broker used only for signaling
SESSION_ID = "magpie/examples/webrtc"         # shared rendezvous name — must match subscriber
TOPIC      = "robot/state"                    # topic to publish data on


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # WebRTCConnection.with_mqtt() creates the MQTT signaling connection and
    # WebRTCConnection in one step.  For broker-less LAN use with_zmq() instead:
    conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", 
                                    SESSION_ID, 
                                    bind=True,
                                    reconnect=True,
                                    options=WebRTCOptions(
                                        stun_servers=[],            # disable stun server in local network for faster connections
                                    ) 
    )

    # conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID)

    # optional WebRTC connection options:
    # from luxai.magpie.transport.webrtc import WebRTCOptions, WebRTCTurnServer
    # conn = WebRTCConnection.with_mqtt(
    #     BROKER_URI, SESSION_ID,
    #     options=WebRTCOptions(
    #         stun_servers=["stun:stun.l.google.com:19302"],
    #         # turn_servers=[WebRTCTurnServer(url="turn:myturn.server:3478",
    #         #                                username="u", credential="p")],
    #     ),
    # )

    if not conn.connect():
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
