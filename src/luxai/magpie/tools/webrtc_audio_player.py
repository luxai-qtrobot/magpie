#!/usr/bin/env python3
"""
magpie-audio-player-webrtc — receive and play a WebRTC audio stream.

Usage:
    magpie-audio-player-webrtc <session_id> [options]
    magpie-audio-player-webrtc my-session --signaling mqtt://127.0.0.1:1883
"""
import sys
import time
import argparse
from collections import deque
from time import perf_counter

try:
    import numpy as np
    import sounddevice as sd
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Audio player requires sounddevice and numpy. Please install with:\n"
        "  pip install \"luxai-magpie[audio]\""
    )
    sys.exit(1)

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader  # noqa: F401
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "WebRTC transport is not installed. Please install it with:\n"
        "  pip install \"luxai-magpie[webrtc]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.frames.audio import AudioFrameRaw
from luxai.magpie.tools._webrtc_tools_common import build_signaler, webrtc_options_type, build_webrtc_options
from luxai.magpie.tools._mqtt_tools_common import mqtt_params_type


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-audio-player-webrtc",
        description="Receive and play a WebRTC audio stream",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the capture side (e.g. my-robot)")
    parser.add_argument("topic", type=str, nargs="?", default="audio",
                        help="Audio topic path to subscribe to (default: audio)")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("--device", type=str, default=None,
                        help="sounddevice output device index or name. Uses default if omitted.")
    parser.add_argument("--latency", type=str, default="low",
                        help="sounddevice latency: 'low', 'high', or float seconds (default: low)")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="Signaling connection timeout in seconds, MQTT only (default: 10).")
    parser.add_argument("--mqtt-params", type=mqtt_params_type, default=None,
                        metavar="JSON|@FILE",
                        help="MQTT signaling options (auth, TLS, …) as JSON or @file.json.")
    parser.add_argument("--webrtc-options", type=webrtc_options_type, default=None,
                        metavar="JSON|@FILE",
                        help="WebRTC options (TURN servers, codecs, …) as JSON or @file.json.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable DEBUG logging / show chunk rate statistics.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    from dataclasses import replace as _dc_replace
    from luxai.magpie.transport.webrtc import WebRTCOptions
    base_opts = build_webrtc_options(args.webrtc_options, args.signaling) or WebRTCOptions()
    if not base_opts.audio_topics:
        base_opts = _dc_replace(base_opts, audio_topics=[args.topic])

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-aplay",
                              timeout=args.timeout, bind=args.bind,
                              mqtt_params=args.mqtt_params)
    conn = WebRTCConnection(signaler=signaler, reconnect=True, options=base_opts)
    sub = WebRtcStreamReader(conn, topic=args.topic)
    Logger.info(f"magpie-audio-player-webrtc: waiting for '{args.topic}' on session '{args.session_id}'")

    out_stream = None
    chunk_rates = deque(maxlen=20)
    prev_t = None

    try:
        conn.connect()
        while True:
            try:
                frame, _ = sub.read(timeout=2.0)
            except TimeoutError:
                continue

            if not isinstance(frame, AudioFrameRaw):
                continue

            samples = np.frombuffer(frame.data, dtype=np.int16)
            if frame.channels > 1:
                n = samples.size // frame.channels
                samples = samples[: n * frame.channels].reshape((n, frame.channels))

            if out_stream is None:
                out_stream = sd.OutputStream(
                    device=args.device,
                    samplerate=frame.sample_rate,
                    channels=frame.channels,
                    dtype="int16",
                    blocksize=0,
                    latency=args.latency,
                )
                out_stream.start()
                Logger.info(
                    f"magpie-audio-player-webrtc: playing at {frame.sample_rate} Hz, "
                    f"{frame.channels} ch"
                )

            if args.verbose:
                now = perf_counter()
                if prev_t is not None:
                    dt = now - prev_t
                    if dt > 0:
                        chunk_rates.append(1.0 / dt)
                        avg = sum(chunk_rates) / len(chunk_rates)
                        Logger.info(
                            f"magpie-audio-player-webrtc: {samples.shape[0]} samples "
                            f"({avg:.1f} chunks/s, {frame.sample_rate} Hz)"
                        )
                prev_t = now

            try:
                out_stream.write(samples)
            except Exception as e:
                Logger.debug(f"magpie-audio-player-webrtc: write error: {e}")

    except KeyboardInterrupt:
        Logger.info("magpie-audio-player-webrtc: interrupted.")

    if out_stream is not None:
        out_stream.stop()
        out_stream.close()
    sub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
