"""
WebRTC Video Writer example.

Streams a camera frame to the remote peer over a native WebRTC RTP video track
(H.264 by default).  The topic must be declared in ``WebRTCOptions.video_topics``
so both sides pre-negotiate the RTP transceiver in the SDP offer/answer.

Usage (run together with webrtc_video_reader.py):
    Terminal 1 (robot):    python examples/webrtc/webrtc_video_writer.py
    Terminal 2 (operator): python examples/webrtc/webrtc_video_reader.py
"""

import cv2

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match reader
HTTP_SIGNAL_URL = "http://127.0.0.1:8000/signal"  # example HTTP relay
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

    # HTTP signaling: start http_signaling_server.py and switch both peers.
    # Comment out the with_zmq() block above and uncomment this block:
    # conn = WebRTCConnection.with_http(
    #     HTTP_SIGNAL_URL, SESSION_ID,
    #     reconnect=True,
    #     options=WebRTCOptions(stun_servers=[], video_topics=[VIDEO_TOPIC]),
    # )

    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    pub = WebRtcStreamWriter(conn)
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
