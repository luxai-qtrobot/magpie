import sys
from dataclasses import dataclass
from logging import Logger
from magpie.frames.frame import Frame

@dataclass
class ImageFrameRaw(Frame):

    data: bytes = b''
    format: str = 'raw'

    def __post_init__(self):
        super().__post_init__()

    def __str__(self):
        return f"{self.name}(size: {len(self.data)}, format: {self.format})"
    

@dataclass
class ImageFrameCV(ImageFrameRaw):
    def __post_init__(self):
        super().__post_init__()

    @classmethod
    def from_cv_image(cls, cv_image: any, format='.jpg'):
        try:
            import cv2
            import numpy as np
        except ImportError as e:
            Logger.error(f"Could not import cv2. Please install it using 'pip install opencv-python'.")
            sys.exit()
        # Encode the frame as a JPEG to serialize it
        _, buffer = cv2.imencode(format, cv_image)
        return cls(data=buffer.tobytes(), format=format)      

    def to_cv_image(self):
        try:
            import cv2
            import numpy as np
        except ImportError as e:
            Logger.error(f"Could not import cv2. Please install it using 'pip install opencv-python'.")
            sys.exit()

        # Convert the bytes to a NumPy array and decode it to get the image
        np_arr = np.frombuffer(self.data, np.uint8)
        return cv2.imdecode(np_arr, cv2.IMREAD_COLOR)