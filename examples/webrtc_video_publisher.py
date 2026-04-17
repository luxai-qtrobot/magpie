"""
WebRTC Video Publisher example.

Streams a camera frame to the remote peer over a native WebRTC RTP video track
(H.264 by default).  The topic must be declared in ``WebRTCOptions.video_topics``
so both sides pre-negotiate the RTP transceiver in the SDP offer/answer.

Usage (run together with webrtc_video_subscriber.py):
    Terminal 1 (robot):    python examples/webrtc_video_publisher.py
    Terminal 2 (operator): python examples/webrtc_video_subscriber.py
"""

import cv2

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match subscriber
VIDEO_TOPIC = "/camera/color/image"             # topic for the RTP video track


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5555",
        SESSION_ID,
        bind=True,
        reconnect=True,
        options=WebRTCOptions(
            stun_servers=[],                    # disable STUN for local network
            video_topics=[VIDEO_TOPIC],         # declare the RTP video track
        )
    )

    # MQTT signaling for internet/cross-network use:
    # conn = WebRTCConnection.with_mqtt(
    #     "mqtt://broker.hivemq.com:1883", SESSION_ID,
    #     options=WebRTCOptions(video_topics=[VIDEO_TOPIC]),
    # )

    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

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
                pub.write(frame, topic=VIDEO_TOPIC)   # topic selects the RTP track
        except KeyboardInterrupt:
            Logger.info("stopping...")
            break

    cap.release()
    pub.close()
    conn.disconnect()
