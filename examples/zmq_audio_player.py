import argparse
import os, sys
import time
from time import perf_counter
from collections import deque

import numpy as np
import sounddevice as sd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.utils.logger import Logger
from magpie.nodes.sink_node import SinkNode
from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber
from magpie.frames.audio import AudioFrameRaw, AudioFrameFlac


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
        self.dtype = None

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

    def _init_stream_from_frame(self, frame: AudioFrameRaw):
        self.samplerate = frame.sample_rate
        self.channels = frame.channels
        self.dtype = self._dtype_from_bitdepth(frame.bit_depth)

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
            f"({self.samplerate} Hz, {self.channels} ch, {self.dtype}, format={frame.format})"
        )

    def process(self):        
        result = self.stream_reader.read()
        if not result:
            return

        msg, topic = result
        frame = AudioFrameRaw.from_dict(msg)
        if frame.format == 'FLAC':
            # ensure we use the FLAC subclass API
            frame_flac = AudioFrameFlac(**frame.__dict__)
            byte_array = frame_flac.to_pcm()
        else:
            byte_array = frame.data

        byte_array = frame.data
        if not byte_array:
            return
    
        # Lazy-init stream when first frame arrives
        if self.stream is None:
            self._init_stream_from_frame(frame)

        # Interpret bytes as PCM according to bit depth
        # Currently we only properly support PCM 16-bit
        samples = np.frombuffer(byte_array, dtype=np.int16)

        channels = frame.channels
        if channels > 1:
            num_frames = samples.size // channels
            samples = samples[: num_frames * channels]
            samples = samples.reshape((num_frames, channels))

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
                        f"({avg_cps:.1f} chunks/s, rate={frame.sample_rate})"
                    )
            self.prev_time = now

        self.stream.write(samples)

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


if __name__ == '__main__':
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

    args = parser.parse_args()

    node = ZmqAudioPlayer(
        name='ZmqAudioPlayer',
        stream_reader=ZMQSubscriber(endpoint=args.endpoint,
                                    topic=args.topic,
                                    queue_size=0),
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
