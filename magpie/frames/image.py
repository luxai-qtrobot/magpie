from dataclasses import dataclass, field, fields
from logging import Logger
import sys
from magpie.frames.frame import Frame

@dataclass
class ImageFrameRaw(Frame):

    data: bytes = b''
    format: str = "raw"

    def __post_init__(self):
        super().__post_init__()

    def __str__(self):
        return f"{self.name}(size: {len(self.data)}, format: {self.format})"
    
    @classmethod
    def from_cv_image(cls, cv_image: any, format='.jpg'):
        try:
            import cv2
        except ImportError as e:
            Logger.error(f"Could not import cv2. Please install it using 'pip install opencv-python'.")
            sys.exit()
        cls.format = format
        # Encode the frame as a JPEG to serialize it
        _, buffer = cv2.imencode(format, cv_image)
        cls.data = buffer.tobytes() 
        return cls      

