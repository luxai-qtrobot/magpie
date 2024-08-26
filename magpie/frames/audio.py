from dataclasses import dataclass, field, fields
from magpie.frames.frame import Frame

@dataclass
class AudioFrameRaw(Frame):

    chunk: bytes    
    channels: int
    sample_rate: int    
    bit_depth: int = 16
    format: str = "WAV"

    def __post_init__(self):
        super().__post_init__()
        self.num_frames = int(len(self.chunk) / (self.channels * self.bit_depth/8))

    def __str__(self):
        return f"{self.name}(size: {len(self.chunk)}, frames: {self.num_frames}, sample_rate: {self.sample_rate}, channels: {self.channels})"
    


@dataclass
class AudioFrameFlac(AudioFrameRaw):

    def __post_init__(self):
        super().__post_init__()
        self.format = 'FLAC'
 