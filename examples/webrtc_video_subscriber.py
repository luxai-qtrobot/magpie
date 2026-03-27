"""
WebRTC Video Subscriber example.

Receives and displays robot camera frames streamed over a WebRTC media track.

Usage (run together with webrtc_video_publisher.py):
    Terminal 1 (robot):    python examples/webrtc_video_publisher.py
    Terminal 2 (operator): python examples/webrtc_video_subscriber.py
"""

import numpy as np
import cv2

from luxai.magpie.frames.image import ImageFrameRaw, ImageFrameJpeg
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCSubscriber, WebRTCOptions
from luxai.magpie.utils import Logger


BROKER_URI = "mqtt://broker.hivemq.com:1883"   # MQTT broker used only for signaling
SESSION_ID = "magpie/examples/webrtc-video"    # shared rendezvous name — must match publisher


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    # For broker-less LAN use with_zmq() instead:    
    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5555", 
        SESSION_ID, 
        bind=False, 
        reconnect=True, 
        options=WebRTCOptions(
            stun_servers=[],            # disable stun server in local network for faster connections
            use_media_channels=True,    # use native webrtc media channels for video/audio frames
            )
        )

    # conn = WebRTCConnection.with_mqtt(BROKER_URI, SESSION_ID, client_id="magpie-webrtc-vidsub")
    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    # VIDEO_TOPIC is a sentinel for the RTP media track when use_media_channels=True.
    # Use a custom topic string (e.g. "/camera") when use_media_channels=False.
    sub = WebRTCSubscriber(conn, topic=WebRTCSubscriber.VIDEO_TOPIC)

    while True:
        try:
            frame, _ = sub.read(timeout=5.0)
            if isinstance(frame, ImageFrameJpeg):
                arr = cv2.imdecode(np.frombuffer(frame.data, dtype=np.uint8), cv2.IMREAD_COLOR)
            elif isinstance(frame, ImageFrameRaw) and frame.width:
                arr = np.frombuffer(frame.data, dtype=np.uint8).reshape(
                    frame.height, frame.width, frame.channels
                )
            else:
                arr = None
            if arr is not None:
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
