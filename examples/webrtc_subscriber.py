"""
WebRTC Subscriber example.

Receives sensor data from the robot over a WebRTC data channel.

Usage (run together with webrtc_publisher.py):
    Terminal 1 (robot):    python examples/webrtc_publisher.py
    Terminal 2 (operator): python examples/webrtc_subscriber.py
"""

from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCSubscriber, WebRTCOptions
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"
SESSION_ID = "magpie/examples/webrtc"
TOPIC      = "robot/state"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-sub")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    opts = WebRTCOptions(
        session_id=SESSION_ID,
        stun_servers=["stun:stun.l.google.com:19302"],
    )
    conn = WebRTCConnection(signaling=signal_conn, options=opts)
    if not conn.connect(timeout=20.0):
        raise SystemExit("WebRTC handshake timed out.")

    sub = WebRTCSubscriber(conn, topic=TOPIC)

    while True:
        try:
            data, topic = sub.read(timeout=5.0)
            Logger.info(f"received on '{topic}': {data}")
        except TimeoutError:
            Logger.warning("no data — is webrtc_publisher.py running?")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    sub.close()
    conn.disconnect()
    signal_conn.disconnect()
