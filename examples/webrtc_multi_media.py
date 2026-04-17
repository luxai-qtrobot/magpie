"""
WebRTC Multi-Media example.

Demonstrates multiple RTP tracks on a single WebRTC connection:
  - Two video tracks: /camera/color/image  and  /camera/depth/image
  - One audio track:  /mic/audio/stream

Each topic maps to its own RTP transceiver in the SDP negotiation.
Both the publisher (robot side) and subscriber (operator side) declare
the same topic lists in their ``WebRTCOptions`` — the SDP offer/answer
exchanges those lists so each incoming ``on_track`` event is mapped to
the correct topic by index.

This script acts as the **publisher** (robot side).  Run a mirror subscriber
that declares the same options and creates a ``WebRTCSubscriber`` for each topic.

Usage:
    Terminal 1 (robot):    python examples/webrtc_multi_media.py
    Terminal 2 (operator): (create subscribers for each topic, same options)

Requirements: luxai-magpie[webrtc,video,audio]
"""

import cv2
import queue
import threading
import numpy as np
import sounddevice as sd

from luxai.magpie.frames.image import ImageFrameRaw
from luxai.magpie.frames.audio import AudioFrameRaw
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher, WebRTCOptions
from luxai.magpie.utils import Logger


SESSION_ID   = "webrtc-multi"
COLOR_TOPIC  = "/camera/color/image"
DEPTH_TOPIC  = "/camera/depth/image"
AUDIO_TOPIC  = "/mic/audio/stream"
SAMPLE_RATE  = 48000
BLOCK_SIZE   = 960


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    conn = WebRTCConnection.with_zmq(
        "tcp://127.0.0.1:5557",
        SESSION_ID,
        bind=True,
        reconnect=True,
        options=WebRTCOptions(
            stun_servers=[],
            video_topics=[COLOR_TOPIC, DEPTH_TOPIC],
            audio_topics=[AUDIO_TOPIC],
        )
    )

    if not conn.connect():
        raise SystemExit("WebRTC handshake timed out.")

    pub  = WebRTCPublisher(conn)
    stop = threading.Event()

    # ── Audio capture thread ────────────────────────────────────────────────
    audio_q: queue.Queue = queue.Queue(maxsize=8)

    def audio_callback(indata, frames, time_info, status):
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

    def audio_thread():
        with sd.InputStream(samplerate=SAMPLE_RATE, channels=1,
                            dtype="int16", blocksize=BLOCK_SIZE,
                            callback=audio_callback):
            while not stop.is_set():
                try:
                    block = audio_q.get(timeout=0.5)
                    frame = AudioFrameRaw(
                        data=block.tobytes(),
                        sample_rate=SAMPLE_RATE, channels=1, bit_depth=16,
                    )
                    pub.write(frame, topic=AUDIO_TOPIC)
                except queue.Empty:
                    pass

    t = threading.Thread(target=audio_thread, daemon=True)
    t.start()

    # ── Video capture loop (main thread) ───────────────────────────────────
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        raise SystemExit("Could not open camera.")

    Logger.info("Streaming color, depth and audio.  Ctrl-C to stop.")
    try:
        while True:
            ret, bgr = cap.read()
            if ret:
                h, w, c = bgr.shape
                # Color frame
                pub.write(
                    ImageFrameRaw(data=bgr.tobytes(), format="raw",
                                  width=w, height=h, channels=c, pixel_format="BGR"),
                    topic=COLOR_TOPIC,
                )
                # Simulated depth: single-channel greyscale (normally from a depth sensor)
                grey = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
                pub.write(
                    ImageFrameRaw(data=grey.tobytes(), format="raw",
                                  width=w, height=h, channels=1, pixel_format="GRAY"),
                    topic=DEPTH_TOPIC,
                )
    except KeyboardInterrupt:
        Logger.info("stopping...")

    stop.set()
    t.join(timeout=2.0)
    cap.release()
    pub.close()
    conn.disconnect()
