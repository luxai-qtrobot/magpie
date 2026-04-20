# zmq_audio_capture.py
#
# Publishes audio frames over ZeroMQ for ZmqAudioPlayer (your SinkNode).
# Inspired by your zmq video capture code.
#
# Examples:
#   # bind by default (writer binds)
#   python zmq_audio_capture.py tcp://0.0.0.0:5556 /audio
#
#   # connect instead of bind
#   python zmq_audio_capture.py tcp://127.0.0.1:5556 /audio --connect
#
#   # raw PCM int16
#   python zmq_audio_capture.py tcp://0.0.0.0:5556 /audio --encoder raw
#
#   # flac
#   python zmq_audio_capture.py tcp://0.0.0.0:5556 /audio --encoder flac

import argparse
import sys
import time
import queue
from io import BytesIO

try:
    import numpy as np
    import sounddevice as sd
    import soundfile as sf
except ImportError:
    from luxai.magpie.utils.logger import Logger
    Logger.error(
        "Could not import required audio dependencies. Please install with:\n"
        "  pip install \"luxai-magpie[audio]\""
    )
    sys.exit(1)

from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from luxai.magpie.nodes.source_node import SourceNode
from luxai.magpie.transport.zmq.zmq_stream_writer import ZmqStreamWriter
from luxai.magpie.frames import AudioFrameRaw, AudioFrameFlac


class ZmqAudioCapture(SourceNode):
    """
    Captures audio via sounddevice and publishes frames via ZmqStreamWriter.
    Produces AudioFrameRaw (PCM int16) or AudioFrameFlac (compressed).
    """

    def setup(
        self,
        device=None,
        sample_rate=16000,
        channels=1,
        blocksize=1024,
        latency="low",
        encoder="raw",
        topic="/audio",
        bit_depth=16,
    ):
        self.device = device
        self.sample_rate = int(sample_rate)
        self.channels = int(channels)
        self.blocksize = int(blocksize)
        self.latency = latency
        self.encoder = encoder
        self.topic = topic
        self.frame_gid = get_uinque_id()
        self._frame_id_counter = 0

        # We publish int16 PCM for RAW and also encode FLAC from int16.
        # Keep bit_depth in metadata for the player (_dtype_from_bitdepth).
        if bit_depth not in (8, 16, 24, 32):
            raise ValueError("bit_depth must be one of: 8, 16, 24, 32")
        self.bit_depth = int(bit_depth)

        if self.encoder == "flac" and sf is None:
            raise ImportError("soundfile is required for FLAC. Install with: pip install soundfile")

        # Internal queue for audio blocks coming from callback.
        # Keep it small and drop old data to favor "latest".
        self._q: queue.Queue[np.ndarray] = queue.Queue(maxsize=4)
        self._stream = None

        self._start_stream()

        Logger.info(
            f"{self.name} initialized with sr={self.sample_rate} Hz, ch={self.channels}, "
            f"blocksize={self.blocksize}, encoder={self.encoder}, topic={self.topic}"
        )
        Logger.info(
            f"{self.name} streaming audio on {self.stream_writer.endpoint} "
            f"({'bind' if getattr(self.stream_writer, 'bind', False) else 'connect'})"
        )

    def _start_stream(self):
        dtype = "int16" if self.bit_depth in (16,) else "int32" if self.bit_depth in (24, 32) else "int8"

        def callback(indata, frames, time_info, status):
            if status:
                # sounddevice status is not always fatal, but it’s useful info
                Logger.debug(f"{self.name} audio callback status: {status}")

            # Ensure we keep a copy because indata is reused by sounddevice
            block = np.array(indata, copy=True)

            # Drop oldest if queue is full (latest delivery behavior)
            try:
                self._q.put_nowait(block)
            except queue.Full:
                try:
                    _ = self._q.get_nowait()
                except queue.Empty:
                    pass
                try:
                    self._q.put_nowait(block)
                except queue.Full:
                    pass  # if still full, just drop

        self._stream = sd.InputStream(
            device=self.device,
            samplerate=self.sample_rate,
            channels=self.channels,
            dtype=dtype,
            blocksize=self.blocksize,
            latency=self.latency,
            callback=callback,
        )
        self._stream.start()

    def _encode_raw(self, samples: np.ndarray) -> AudioFrameRaw:
        # Ensure contiguous bytes
        if not samples.flags["C_CONTIGUOUS"]:
            samples = np.ascontiguousarray(samples)

        # For multi-channel, samples is (N, C). For mono, (N,) or (N,1).
        if samples.ndim == 1:
            ch = 1
        else:
            ch = samples.shape[1]

        # Normalize to int16 for consistency with your player raw path
        if samples.dtype != np.int16:
            # Convert safely (best-effort)
            if np.issubdtype(samples.dtype, np.floating):
                # Assume float -1..1
                samples = np.clip(samples, -1.0, 1.0)
                samples = (samples * 32767.0).astype(np.int16)
            else:
                samples = samples.astype(np.int16)

        frame = AudioFrameRaw(
            data=samples.tobytes(),
            sample_rate=self.sample_rate,
            channels=ch,
            bit_depth=16,
        )
        return frame

    def _encode_flac(self, samples: np.ndarray) -> AudioFrameFlac:
        # Convert to int16 since your player decodes FLAC to int16
        if samples.dtype != np.int16:
            if np.issubdtype(samples.dtype, np.floating):
                samples = np.clip(samples, -1.0, 1.0)
                samples = (samples * 32767.0).astype(np.int16)
            else:
                samples = samples.astype(np.int16)

        # soundfile expects shape (frames, channels) for multichannel
        if samples.ndim == 1 and self.channels > 1:
            # If somehow flattened, reshape best-effort
            n = samples.size // self.channels
            samples = samples[: n * self.channels].reshape((n, self.channels))

        buf = BytesIO()
        # subtype for FLAC is typically PCM_16; FLAC container handles it.
        sf.write(buf, samples, self.sample_rate, format="FLAC", subtype="PCM_16")
        data = buf.getvalue()

        frame = AudioFrameFlac(
            data=data,
            sample_rate=self.sample_rate,
            channels=self.channels if samples.ndim > 1 else 1,
            bit_depth=16,
        )
        return frame

    def process(self):
        # Drain queue to get the latest chunk (match delivery="latest")
        latest = None
        while True:
            try:
                latest = self._q.get_nowait()
            except queue.Empty:
                break

        if latest is None:
            return

        if self.encoder == "raw":
            frame = self._encode_raw(latest)
        elif self.encoder == "flac":
            frame = self._encode_flac(latest)
        else:
            raise ValueError("Unsupported encoder. Use: raw, flac")

        frame.gid = self.frame_gid
        frame.id = self._frame_id_counter        
        self.stream_writer.write(frame.to_dict(), topic=self.topic)
        self._frame_id_counter += 1

    def terminate(self):
        try:
            if self._stream is not None:
                self._stream.stop()
                self._stream.close()
        except Exception as e:
            Logger.error(f"{self.name} error while closing audio input stream: {e}")

        try:
            super().terminate()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "endpoint",
        help="ZeroMQ publishing socket endpoint (e.g. tcp://0.0.0.0:5556)",
        type=str,
    )
    parser.add_argument(
        "topic",
        help="ZeroMQ publishing topic on endpoint (e.g. /audio)",
        type=str,
    )

    parser.add_argument(
        "--bind",
        action="store_true",
        help="Bind the writer socket instead of connecting (default: connect).",
    )

    parser.add_argument(
        "--device",
        help="sounddevice input device (id or name). If omitted, uses default input device.",
        default=None,
    )
    parser.add_argument(
        "--samplerate",
        help="Audio sample rate (e.g. 16000, 48000)",
        type=int,
        default=16000,
    )
    parser.add_argument(
        "--channels",
        help="Number of input channels",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--blocksize",
        help="Audio blocksize in frames (chunk size)",
        type=int,
        default=1024,
    )
    parser.add_argument(
        "--latency",
        help="sounddevice latency ('low', 'high', or float seconds)",
        type=str,
        default="low",
    )
    parser.add_argument(
        "--encoder",
        choices=["raw", "flac"],
        default="raw",
        help="Encoding backend: 'raw' for PCM int16, 'flac' for compressed FLAC",
    )

    args = parser.parse_args()

    node = ZmqAudioCapture(
        name="MagpieAudioCapture",
        stream_writer=ZmqStreamWriter(
            endpoint=args.endpoint,
            bind=args.bind,
            queue_size=0,
            delivery="latest",
        ),
        setup_kwargs={
            "device": args.device,
            "sample_rate": args.samplerate,
            "channels": args.channels,
            "blocksize": args.blocksize,
            "latency": args.latency,
            "encoder": args.encoder,
            "topic": args.topic,
            "bit_depth": 16,
        },
    )

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass

    Logger.info("Closing...")
    node.terminate()


if __name__ == "__main__":
    main()
