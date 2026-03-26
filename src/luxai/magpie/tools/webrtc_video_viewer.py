#!/usr/bin/env python3
"""
magpie-video-viewer-webrtc — receive and display a WebRTC video stream.

Usage:
    magpie-video-viewer-webrtc <session_id> [options]
    magpie-video-viewer-webrtc my-session --signaling mqtt://127.0.0.1:1883
"""
import sys
import time
import argparse
from collections import deque
from time import perf_counter

try:
    import cv2
    import numpy as np
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Video viewer requires OpenCV. Please install it with:\n"
        "  pip install \"luxai-magpie[video]\""
    )
    sys.exit(1)

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCSubscriber  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.tools._webrtc_tools_common import build_signaler


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-video-viewer-webrtc",
        description="Receive and display a WebRTC video stream",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the capture side (e.g. my-robot)")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging / show FPS overlay.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-vview",
                              timeout=args.timeout, bind=args.bind)
    conn = WebRTCConnection(signaler=signaler)
    conn.connect()

    sub = WebRTCSubscriber(conn, topic=WebRTCSubscriber.VIDEO_TOPIC)
    Logger.info(f"magpie-video-viewer-webrtc: waiting for video on session '{args.session_id}'")

    fps_window = deque(maxlen=30)
    prev_t = None
    window_name = f"WebRTC — {args.session_id}"

    try:
        while True:
            try:
                frame, _ = sub.read(timeout=2.0)
            except TimeoutError:
                # Check if window was closed
                if cv2.getWindowProperty(window_name, cv2.WND_PROP_VISIBLE) == 0:
                    break
                continue

            if not isinstance(frame, ImageFrameRaw) or not frame.width:
                continue

            image = np.frombuffer(frame.data, dtype=np.uint8).reshape(
                frame.height, frame.width, frame.channels
            )

            if args.verbose and prev_t is not None:
                fps_window.append(1.0 / max(perf_counter() - prev_t, 1e-9))
                avg_fps = int(sum(fps_window) / len(fps_window))
                cv2.putText(
                    image,
                    f"{frame.width}x{frame.height}  {avg_fps} fps",
                    (10, frame.height - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (112, 82, 204), 1, cv2.LINE_AA,
                )
            prev_t = perf_counter()

            cv2.imshow(window_name, image)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    except KeyboardInterrupt:
        Logger.info("magpie-video-viewer-webrtc: interrupted.")

    cv2.destroyAllWindows()
    sub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
