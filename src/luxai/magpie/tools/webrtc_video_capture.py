#!/usr/bin/env python3
"""
magpie-video-capture-webrtc — capture from a camera and stream over WebRTC.

Usage:
    magpie-video-capture-webrtc <session_id> [options]
    magpie-video-capture-webrtc my-session --signaling mqtt://127.0.0.1:1883 -c 0 -f 30
"""
import sys
import time
import argparse

try:
    import cv2
    import numpy as np  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Video capture requires OpenCV. Please install it with:\n"
        "  pip install \"luxai-magpie[video]\""
    )
    sys.exit(1)

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.tools._webrtc_tools_common import build_signaler, webrtc_options_type, build_webrtc_options
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-video-capture-webrtc",
        description="Capture from a camera and stream video over a WebRTC media track",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the viewer (e.g. my-robot)")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("-c", "--camera", type=int, default=0,
                        help="OpenCV camera device index (default: 0)")
    parser.add_argument("-f", "--framerate", type=int, default=30,
                        help="Target capture frame rate (default: 30)")
    parser.add_argument("-s", "--size", nargs=2, type=int, default=[1280, 720],
                        metavar=("W", "H"),
                        help="Capture resolution width height (default: 1280 720)")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        metavar="JSON|@FILE",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=webrtc_options_type, default=None,
                        metavar="JSON|@FILE",
                        help="WebRTC options (TURN servers, codecs, …) as JSON or @file.json.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    cap = cv2.VideoCapture(args.camera)
    if not cap.isOpened():
        Logger.error(f"magpie-video-capture-webrtc: could not open camera {args.camera}")
        sys.exit(1)

    w, h = args.size
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, w)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, h)
    cap.set(cv2.CAP_PROP_FPS, args.framerate)
    actual_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    Logger.info(f"magpie-video-capture-webrtc: camera {args.camera} @ {actual_w}x{actual_h} {args.framerate}fps")

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-vcap",
                              timeout=args.timeout, bind=args.bind,
                              mqtt_params=args.mqtt_params)
    conn = WebRTCConnection(signaler=signaler, reconnect=True,
                            options=build_webrtc_options(args.webrtc_options))
    pub = WebRTCPublisher(conn)
    Logger.info(f"magpie-video-capture-webrtc: streaming on session '{args.session_id}'")

    frame_period = 1.0 / max(1, args.framerate)
    try:
        conn.connect()
        while True:
            t = time.time()
            ret, cv_image = cap.read()
            if ret:
                fh, fw, fc = cv_image.shape
                frame = ImageFrameRaw(
                    data=cv_image.tobytes(), format="raw",
                    width=fw, height=fh, channels=fc, pixel_format="BGR",
                )
                pub.write(frame)
            elapsed = time.time() - t
            if elapsed < frame_period:
                time.sleep(frame_period - elapsed)
    except KeyboardInterrupt:
        Logger.info("magpie-video-capture-webrtc: interrupted.")

    cap.release()
    pub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
