"""
WebRTC Video Reader example.

Receives and displays camera frames streamed over a native WebRTC RTP video track.
The topic must match what the writer declared in ``WebRTCOptions.video_topics``.

Usage (run together with webrtc_video_publisher.py):
    Terminal 1 (robot):    python examples/webrtc_video_publisher.py
    Terminal 2 (operator): python examples/webrtc_video_subscriber.py
"""

import numpy as np
import cv2

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match writer
VIDEO_TOPIC = "/camera/color/image"             # must match writer's video_topics entry


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5555",
        SESSION_ID,
        bind=False,
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

    # Topic matches the RTP video track declared in options
    sub = WebRtcStreamReader(conn, topic=VIDEO_TOPIC)

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
