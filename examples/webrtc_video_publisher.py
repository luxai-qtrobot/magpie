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

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"   # MQTT broker used only for signaling
SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match subscriber


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # For broker-less LAN use with_zmq() instead:
    conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", SESSION_ID, bind=True)
    # conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID, client_id="magpie-webrtc-vidpub")

    # optional WebRTC connection options:
    # from luxai.magpie.transport.webrtc import WebRTCOptions
    # conn = WebRTCConnection.with_mqtt(
    #     BROKER_URI, SESSION_ID,
    #     options=WebRTCOptions(
    #         stun_servers=["stun:stun.l.google.com:19302"],
    #         video_codec="H264",
    #         video_bitrate=2000,
    #     ),
    # )

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
                h, w, c = cv_image.shape
                frame = ImageFrameRaw(
                    data=cv_image.tobytes(), format="raw",
                    width=w, height=h, channels=c, pixel_format="BGR",
                )
                pub.write(frame)
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    cap.release()
    pub.close()
    conn.disconnect()
