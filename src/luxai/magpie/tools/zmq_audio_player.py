import argparse
import os, sys
import time
from time import perf_counter
from collections import deque

try:
    import numpy as np
    import sounddevice as sd
except ImportError:
    from luxai.magpie.utils import Logger
    Logger.error(
        "Could not import required audio dependencies. Please install with:\n"
        "  pip install \"luxai-magpie[audio]\""
    )
    sys.exit(1)

from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZmqStreamReader
from luxai.magpie.frames import Frame, AudioFrameRaw, AudioFrameFlac


class ZmqAudioPlayer(SinkNode):

    def setup(self, latency='low', show_statistics=False):
        """
        latency    : 'low', 'high', or float seconds
        """
        self.latency = latency
        self.show_statistics = show_statistics

        # These will be set once we see the first frame
        self.stream = None
        self.samplerate = None
        self.channels = None
        self.dtype = 'int16'

        # For simple throughput statistics (chunks per second)
        self.prev_time = None
        self.chunk_rates = deque(maxlen=20)

        Logger.info(f"{self.name} waiting for audio frames from {self.stream_reader.endpoint}...")

    def _dtype_from_bitdepth(self, bit_depth: int) -> str:
        # Extend this mapping if you add more formats later
        if bit_depth == 8:
            return 'int8'
        if bit_depth == 16:
            return 'int16'
        if bit_depth in (24, 32):            
            return 'int32'
        # Fallback
        return 'int16'


    def _init_stream(self, samplerate: int, channels: int):
        self.samplerate = samplerate
        self.channels = channels

        self.stream = sd.OutputStream(
            samplerate=self.samplerate,
            channels=self.channels,
            dtype=self.dtype,
            blocksize=0,
            latency=self.latency,
        )
        self.stream.start()

        Logger.info(
            f"{self.name} playing audio from {self.stream_reader.endpoint} "
            f"({self.samplerate} Hz, {self.channels} ch, {self.dtype})"
        )


    def process(self):        
        result = self.stream_reader.read()
        if not result:
            return

        msg, topic = result

        # pick AudioFrameRaw or AudioFrameFlac automatically
        try:
            frame = Frame.from_dict(msg)
        except Exception as e:
            Logger.warning(f"{self.name} failed to deserialize frame: {e}")
            return

        # --- Ensure it's an audio frame ---
        if not isinstance(frame, (AudioFrameRaw, AudioFrameFlac)):
            Logger.warning(f"{self.name} received unsupported frame type: {frame.name}")
            return
        
        # ============================================================
        #  FLAC PATH : Decode compressed FLAC to PCM (np.int16)
        # ============================================================
        if isinstance(frame, AudioFrameFlac):            
            import io
            import soundfile as sf    

            buf = io.BytesIO(frame.data)
            samples, sr = sf.read(buf, dtype='int16', always_2d=False)
            # Update metadata to reflect decoded audio
            frame.sample_rate = sr
            frame.bit_depth = 16
            if samples.ndim == 1:
                frame.channels = 1
            else:
                frame.channels = samples.shape[1]

        # ============================================================
        #  RAW PCM PATH : Convert bytes directly to int16 NumPy array
        # ============================================================
        else:   # AudioFrameRaw
            samples = np.frombuffer(frame.data, dtype=np.int16)

            # reshaping if multi-channel PCM
            if frame.channels > 1:
                num_frames = samples.size // frame.channels
                samples = samples[: num_frames * frame.channels]
                samples = samples.reshape((num_frames, frame.channels))
                

        # --- Lazy-init audio stream once we know samplerate/channels ---
        if self.stream is None:
            # If FLAC, we already updated frame.sample_rate / frame.channels above
            self._init_stream(samplerate=frame.sample_rate, channels=frame.channels)

            # For FLAC, if samples is 1D but channels > 1, reshape now
        if frame.channels > 1 and samples.ndim == 1:
            num_frames = samples.size // frame.channels
            samples = samples[: num_frames * frame.channels]
            samples = samples.reshape((num_frames, frame.channels))

        # --- Statistics (unchanged) ---
        if self.show_statistics:
            now = perf_counter()
            if self.prev_time is not None:
                dt = now - self.prev_time
                if dt > 0:
                    cps = 1.0 / dt
                    self.chunk_rates.append(cps)
                    avg_cps = sum(self.chunk_rates) / len(self.chunk_rates)
                    Logger.info(
                        f"{self.name}: {samples.shape[0]} samples "
                        f"({avg_cps:.1f} chunks/s, rate={frame.sample_rate}, format={frame.format})"
                    )
            self.prev_time = now

        # --- Play audio ---
        try:
            self.stream.write(samples)
        except Exception as e:
            Logger.debug(f"{self.name} error while writing to audio stream: {e}")
        

    def terminate(self):
        # Clean up audio resources
        try:
            if hasattr(self, "stream") and self.stream:
                self.stream.stop()
                self.stream.close()
        except Exception as e:
            Logger.error(f"{self.name} error while closing audio stream: {e}")

        try:
            super().terminate()
        except Exception:
            pass


def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "endpoint",
        help="ZeroMQ subscribing socket endpoint (e.g. tcp://127.0.0.1:5556)",
        type=str,
    )
    parser.add_argument(
        "topic",
        help="ZeroMQ subscribing topic on endpoint (e.g. /mytopic)",
        type=str,
    )
    parser.add_argument(
        "--latency",
        help="desired latency for sounddevice (e.g. 'low', 'high', or float seconds)",
        type=str,
        default="low",
    )
    parser.add_argument(
        "-v", "--verbose",
        help="show statistics for received audio chunks",
        action="store_true",
    )

    parser.add_argument(
        "--bind",
        action="store_true",
        help="Bind the reader socket instead of connecting (default: connect).",
    )

    args = parser.parse_args()

    node = ZmqAudioPlayer(
        name='MagpieAudioPlayer',
        stream_reader=ZmqStreamReader(endpoint=args.endpoint,
                                    topic=args.topic,
                                    bind=args.bind,
                                    queue_size=1,                                    
                                    delivery="latest"),
                                    setup_kwargs={
                                        'latency': args.latency,
                                        'show_statistics': args.verbose,
                                    })

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        pass

    Logger.info("Closing...")
    node.terminate()


# Optional, purely for manual `python -m` usage:
if __name__ == "__main__":
    main()