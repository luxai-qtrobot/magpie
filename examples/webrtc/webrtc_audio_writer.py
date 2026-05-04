"""
WebRTC Audio Writer example.

Captures microphone audio and streams it over a native WebRTC RTP audio track (Opus).
The topic must be declared in ``WebRTCOptions.audio_topics`` so both sides
pre-negotiate the RTP transceiver in the SDP offer/answer.

Usage (run together with webrtc_audio_subscriber.py):
    Terminal 1 (robot):    python examples/webrtc_audio_publisher.py
    Terminal 2 (operator): python examples/webrtc_audio_subscriber.py

Requirements: luxai-magpie[webrtc,audio]
"""

import queue
import numpy as np
import sounddevice as sd

from luxai.magpie.frames.audio import AudioFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID  = "magpie/examples/webrtc-audio"
AUDIO_TOPIC = "/mic/audio/stream"
SAMPLE_RATE = 48000
CHANNELS    = 1
BLOCK_SIZE  = 960   # 20 ms @ 48 kHz — native Opus frame size


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5556",
        SESSION_ID,
        bind=True,
        reconnect=True,
        options=WebRTCOptions(
            stun_servers=[],
            audio_topics=[AUDIO_TOPIC],
        )
    )

    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    pub = WebRtcStreamWriter(conn)
    audio_q: queue.Queue = queue.Queue(maxsize=8)

    def audio_callback(indata, frames, time_info, status):
        if status:
            Logger.debug(f"sounddevice status: {status}")
        block = np.array(indata, copy=True)
        if block.dtype != np.int16:
            block = np.clip(block, -1.0, 1.0)
            block = (block * 32767.0).astype(np.int16)
        try:
            audio_q.put_nowait(block)
        except queue.Full:
            try:
                audio_q.get_nowait()
            except queue.Empty:
                pass
            try:
                audio_q.put_nowait(block)
            except queue.Full:
                pass

    stream = sd.InputStream(
        samplerate=SAMPLE_RATE, channels=CHANNELS,
        dtype="int16", blocksize=BLOCK_SIZE,
        callback=audio_callback,
    )
    stream.start()
    Logger.info(f"Streaming audio on '{AUDIO_TOPIC}' …  Ctrl-C to stop.")

    try:
        while True:
            try:
                block = audio_q.get(timeout=1.0)
            except queue.Empty:
                continue
            frame = AudioFrameRaw(
                data=block.tobytes(),
                sample_rate=SAMPLE_RATE,
                channels=CHANNELS,
                bit_depth=16,
            )
            pub.write(frame, topic=AUDIO_TOPIC)
    except KeyboardInterrupt:
        Logger.info("stopping...")

    stream.stop()
    stream.close()
    pub.close()
    conn.disconnect()
