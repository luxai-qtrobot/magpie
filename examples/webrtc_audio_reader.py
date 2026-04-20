"""
WebRTC Audio Reader example.

Receives microphone audio streamed over a native WebRTC RTP audio track and plays
it through the local speaker.

Usage (run together with webrtc_audio_publisher.py):
    Terminal 1 (robot):    python examples/webrtc_audio_publisher.py
    Terminal 2 (operator): python examples/webrtc_audio_subscriber.py

Requirements: luxai-magpie[webrtc,audio]
"""

import numpy as np
import sounddevice as sd

from luxai.magpie.frames.audio import AudioFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID  = "magpie/examples/webrtc-audio"
AUDIO_TOPIC = "/mic/audio/stream"   # must match writer's audio_topics entry


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5556",
        SESSION_ID,
        bind=False,
        reconnect=True,
        options=WebRTCOptions(
            stun_servers=[],
            audio_topics=[AUDIO_TOPIC],
        )
    )

    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    sub = WebRtcStreamReader(conn, topic=AUDIO_TOPIC)
    out_stream = None

    Logger.info(f"Waiting for audio on '{AUDIO_TOPIC}' …  Ctrl-C to stop.")
    try:
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
                    samplerate=frame.sample_rate,
                    channels=frame.channels,
                    dtype="int16",
                    blocksize=0,
                    latency="low",
                )
                out_stream.start()
                Logger.info(f"Playing at {frame.sample_rate} Hz, {frame.channels} ch")

            try:
                out_stream.write(samples)
            except Exception as e:
                Logger.debug(f"write error: {e}")

    except KeyboardInterrupt:
        Logger.info("stopping...")

    if out_stream is not None:
        out_stream.stop()
        out_stream.close()
    sub.close()
    conn.disconnect()
