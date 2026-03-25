"""
WebRTC Video Subscriber example.

Receives and displays robot camera frames streamed over a WebRTC media track.

Usage (run together with webrtc_video_publisher.py):
    Terminal 1 (robot):    python examples/webrtc_video_publisher.py
    Terminal 2 (operator): python examples/webrtc_video_subscriber.py
"""

import numpy as np
import cv2

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import (
    WebRTCConnection, WebRTCSubscriber, WebRTCOptions
)
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"
SESSION_ID = "magpie/examples/webrtc-video"


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-vidsub")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    opts = WebRTCOptions(
        session_id=SESSION_ID,
        stun_servers=["stun:stun.l.google.com:19302"],
    )
    conn = WebRTCConnection(signaling=signal_conn, options=opts)
    if not conn.connect(timeout=20.0):
        raise SystemExit("WebRTC handshake timed out.")

    # Use the special "video" topic to subscribe to the media track
    sub = WebRTCSubscriber(conn, topic=WebRTCSubscriber.VIDEO_TOPIC)

    while True:
        try:
            frame, _ = sub.read(timeout=5.0)
            if isinstance(frame, ImageFrameRaw) and frame.width:
                arr = np.frombuffer(frame.data, dtype=np.uint8).reshape(
                    frame.height, frame.width, frame.channels
                )
                cv2.imshow("WebRTC video", arr)
                if cv2.waitKey(1) & 0xFF == ord("q"):
                    break
        except TimeoutError:
            Logger.warning("no video — is webrtc_video_publisher.py running?")
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    cv2.destroyAllWindows()
    sub.close()
    conn.disconnect()
    signal_conn.disconnect()
