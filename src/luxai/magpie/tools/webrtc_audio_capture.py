#!/usr/bin/env python3
"""
magpie-audio-capture-webrtc — capture from a microphone and stream over WebRTC.

Usage:
    magpie-audio-capture-webrtc <session_id> [options]
    magpie-audio-capture-webrtc my-session --signaling mqtt://127.0.0.1:1883
    magpie-audio-capture-webrtc my-session --signaling mqtt://127.0.0.1:1883 --samplerate 48000
"""
import sys
import time
import queue
import argparse

try:
    import numpy as np
    import sounddevice as sd
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Audio capture requires sounddevice and numpy. Please install with:\n"
        "  pip install \"luxai-magpie[audio]\""
    )
    sys.exit(1)

try:
    from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter  # noqa: F401
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
        prog="magpie-audio-capture-webrtc",
        description="Capture from a microphone and stream audio over a WebRTC media track",
    )
    parser.add_argument("session_id", type=str,
                        help="Shared session name — must match the player (e.g. my-robot)")
    parser.add_argument("topic", type=str, nargs="?", default="audio",
                        help="Audio topic path to publish to (default: audio)")
    parser.add_argument("--signaling", type=str, default="mqtt://127.0.0.1:1883",
                        metavar="URL",
                        help="Signaling URL: mqtt://host:port or tcp://host:port (ZMQ). "
                             "(default: mqtt://127.0.0.1:1883)")
    parser.add_argument("--bind", action="store_true",
                        help="Bind the ZMQ signaling socket (tcp:// only).")
    parser.add_argument("--device", type=str, default=None,
                        help="sounddevice input device index or name. Uses default if omitted.")
    parser.add_argument("--samplerate", type=int, default=48000,
                        help="Audio sample rate in Hz (default: 48000)")
    parser.add_argument("--channels", type=int, default=1,
                        help="Number of input channels (default: 1)")
    parser.add_argument("--blocksize", type=int, default=960,
                        help="Audio block size in frames per callback (default: 960 = 20ms @ 48kHz)")
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
                        help="Enable DEBUG logging.")

    args = parser.parse_args()
    Logger.set_level("DEBUG" if args.verbose else "INFO")


    audio_queue: queue.Queue = queue.Queue(maxsize=8)

    def audio_callback(indata, frames, time_info, status):
        if status:
            Logger.debug(f"magpie-audio-capture-webrtc: sounddevice status: {status}")
        block = np.array(indata, copy=True)
        try:
            audio_queue.put_nowait(block)
        except queue.Full:
            try:
                audio_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                audio_queue.put_nowait(block)
            except queue.Full:
                pass

    from dataclasses import replace as _dc_replace
    from luxai.magpie.transport.webrtc import WebRTCOptions
    base_opts = build_webrtc_options(args.webrtc_options, args.signaling) or WebRTCOptions()
    if not base_opts.audio_topics:
        base_opts = _dc_replace(base_opts, audio_topics=[args.topic])

    signaler = build_signaler(args.signaling, args.session_id,
                              client_id="magpie-webrtc-acap",
                              timeout=args.timeout, bind=args.bind,
                              mqtt_params=args.mqtt_params)
    conn = WebRTCConnection(signaler=signaler, reconnect=True, options=base_opts)
    pub = WebRtcStreamWriter(conn)
    Logger.info(f"magpie-audio-capture-webrtc: streaming '{args.topic}' on session '{args.session_id}'")

    stream = sd.InputStream(
        device=args.device,
        samplerate=args.samplerate,
        channels=args.channels,
        dtype="int16",
        blocksize=args.blocksize,
        latency=args.latency,
        callback=audio_callback,
    )

    try:
        conn.connect()
        stream.start()
        Logger.info(
            f"magpie-audio-capture-webrtc: capturing at {args.samplerate} Hz, "
            f"{args.channels} ch, blocksize={args.blocksize}"
        )
        while True:
            try:
                block = audio_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if block.dtype != np.int16:
                if np.issubdtype(block.dtype, np.floating):
                    block = np.clip(block, -1.0, 1.0)
                    block = (block * 32767.0).astype(np.int16)
                else:
                    block = block.astype(np.int16)

            if block.ndim == 1:
                ch = 1
            else:
                ch = block.shape[1]

            frame = AudioFrameRaw(
                data=block.tobytes(),
                sample_rate=args.samplerate,
                channels=ch,
                bit_depth=16,
            )
            pub.write(frame, topic=args.topic)

    except KeyboardInterrupt:
        Logger.info("magpie-audio-capture-webrtc: interrupted.")

    stream.stop()
    stream.close()
    pub.close()
    conn.disconnect()


if __name__ == "__main__":
    main()
