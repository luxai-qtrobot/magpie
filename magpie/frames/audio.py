from dataclasses import dataclass, field, fields
from magpie.frames.frame import Frame

@dataclass
class AudioFrameRaw(Frame):

    data: bytes = b''
    channels: int = 1
    sample_rate: int = 16_000   
    bit_depth: int = 16
    format: str = "PCM"

    def __post_init__(self):
        super().__post_init__()
        self.num_frames = int(len(self.data) / (self.channels * self.bit_depth/8))

    def __str__(self):
        return f"{self.name}(size: {len(self.data)}, frames: {self.num_frames}, sample_rate: {self.sample_rate}, channels: {self.channels})"
    


@dataclass
class AudioFrameFlac(AudioFrameRaw):

    def __post_init__(self):
        super().__post_init__()
        self.format = 'FLAC'
 