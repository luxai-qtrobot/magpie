"""
WebRTC Video Publisher example.

Streams robot camera frames to the operator over a WebRTC media track
(H.264 by default).  Frames are encoded natively by aiortc — no msgpack
overhead, with built-in adaptive bitrate and packet-loss recovery.

Usage (run together with webrtc_video_subscriber.py):
    Terminal 1 (robot):    python examples/webrtc_video_publisher.py
    Terminal 2 (operator): python examples/webrtc_video_subscriber.py
"""

import cv2

from luxai.magpie.frames.image import ImageFrameCV
from luxai.magpie.transport import MqttConnection
from luxai.magpie.transport.webrtc import (
    WebRTCConnection, WebRTCPublisher,
    WebRTCOptions,  # optional — uncomment opts block below to use
)
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"   # MQTT broker used only for signaling
SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match subscriber


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    signal_conn = MqttConnection(BROKER_URI, client_id="magpie-webrtc-vidpub")
    if not signal_conn.connect(timeout=10.0):
        raise SystemExit("Could not connect to MQTT broker.")

    # optional WebRTC connection options
    # opts = WebRTCOptions(
    #     stun_servers=["stun:stun.l.google.com:19302"],
    #     video_codec="H264",
    #     video_bitrate=2000,
    # )
    conn = WebRTCConnection(signaling=signal_conn, session_id=SESSION_ID)
    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    # Single publisher handles both video (media track) and data automatically
    pub = WebRTCPublisher(conn)

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        raise SystemExit("Could not open camera.")

    while True:
        try:
            ret, cv_image = cap.read()
            if ret:
                # ImageFrameCV → routed to the WebRTC video media track
                frame = ImageFrameCV.from_cv_image(cv_image)
                pub.write(frame)
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    cap.release()
    pub.close()
    conn.disconnect()
    signal_conn.disconnect()
