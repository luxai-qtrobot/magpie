from dataclasses import dataclass, field, fields
from magpie.frames.frame import Frame

@dataclass
class AudioFrameRaw(Frame):

    channels: int = 1
    sample_rate: int = 16_000   
    bit_depth: int = 16
    format: str = "PCM"
    data: bytes = b''

    def __post_init__(self):
        super().__post_init__()
        # normalize data to bytes
        if isinstance(self.data, list):         
            self.data = bytes(self.data)
        self.num_frames = int(len(self.data) / (self.channels * self.bit_depth/8))

    def __str__(self):
        return f"{self.name}(size: {len(self.data)}, frames: {self.num_frames}, sample_rate: {self.sample_rate}, channels: {self.channels})"
    


@dataclass
class AudioFrameFlac(AudioFrameRaw):

    def __post_init__(self):
        super().__post_init__()
        self.format = 'FLAC'

    @classmethod
    def from_pcm(cls, pcm_bytes: bytes, channels: int, sample_rate: int, bit_depth: int = 16):
        """Create FLAC-compressed frame from raw PCM bytes."""
        try:
            import soundfile as sf
            import numpy as np
        except ImportError:
            raise ImportError("Please install soundfile: pip install soundfile")

        dtype = np.int16 if bit_depth == 16 else np.int32
        samples = np.frombuffer(pcm_bytes, dtype=dtype)
        if channels > 1:
            num_frames = samples.size // channels
            samples = samples[: num_frames * channels]
            samples = samples.reshape((num_frames, channels))

        buf = io.BytesIO()
        sf.write(buf, samples, sample_rate, format='FLAC', subtype='PCM_16')
        return cls(
            data=buf.getvalue(),
            channels=channels,
            sample_rate=sample_rate,
            bit_depth=bit_depth,
        )

    def to_pcm(self) -> bytes:
        """Decode FLAC frame back to raw PCM bytes."""
        try:
            import soundfile as sf
            import numpy as np
        except ImportError:
            raise ImportError("Please install soundfile: pip install soundfile")

        buf = io.BytesIO(self.data)
        samples, sr = sf.read(buf, dtype='int16', always_2d=False)
        self.sample_rate = sr
        if samples.ndim == 1:
            self.channels = 1
        else:
            self.channels = samples.shape[1]
        self.bit_depth = 16
        return samples.tobytes() 