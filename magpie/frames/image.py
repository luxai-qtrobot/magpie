import sys
from dataclasses import dataclass
from logging import Logger
from magpie.frames.frame import Frame

@dataclass
class ImageFrameRaw(Frame):
    data: bytes = b''                  # Encoded or raw pixel buffer
    format: str = 'raw'                # Encoding format: 'raw', 'jpeg', '.jpg', 'png', etc.

    # New optional metadata fields
    width: int = 0                     # Pixel width
    height: int = 0                    # Pixel height
    channels: int = 0                  # 1 (gray), 3 (RGB/BGR), 4 (RGBA)
    pixel_format: str = ''             # 'RGB', 'BGR', 'GRAY', 'NV12', 'YUV420', ...

    def __post_init__(self):
        super().__post_init__()

    def __str__(self):
        size_info = f"{self.width}x{self.height}x{self.channels}" if self.width else "unknown"
        return f"{self.name}(size: {len(self.data)}, dims: {size_info}, format: {self.format})"

    

@dataclass
class ImageFrameCV(ImageFrameRaw):
    def __post_init__(self):
        super().__post_init__()

    @classmethod
    def from_cv_image(cls, cv_image: any, format: str = '.jpg'):
        try:
            import cv2
            import numpy as np
        except ImportError:
            Logger.error("Could not import cv2. Please install it using 'pip install opencv-python'.")
            sys.exit()

        # Infer dimensions and channels from the OpenCV image
        if cv_image.ndim == 2:
            height, width = cv_image.shape
            channels = 1
            pixel_format = 'GRAY'
        elif cv_image.ndim == 3:
            height, width, channels = cv_image.shape
            # OpenCV default color order
            pixel_format = 'BGR'
        else:
            raise ValueError(f"Unsupported cv_image shape: {cv_image.shape}")

        # Encode the frame (e.g. JPEG/PNG) to serialize it
        ok, buffer = cv2.imencode(format, cv_image)
        if not ok:
            raise RuntimeError("cv2.imencode failed for ImageFrameCV")

        return cls(
            data=buffer.tobytes(),
            format=format,
            width=width,
            height=height,
            channels=channels,
            pixel_format=pixel_format,
        )

    def to_cv_image(self):
        try:
            import cv2
            import numpy as np
        except ImportError:
            Logger.error("Could not import cv2. Please install it using 'pip install opencv-python'.")
            sys.exit()

        # Convert the bytes to a NumPy array and decode it to get the image
        np_arr = np.frombuffer(self.data, np.uint8)
        return cv2.imdecode(np_arr, cv2.IMREAD_COLOR)



@dataclass
class ImageFrameJpeg(ImageFrameRaw):
    """
    JPEG-encoded image frame using `simplejpeg`.

    - `data` contains JPEG bytes
    - `format` is "jpeg"
    - width/height/channels/pixel_format are filled from the source ndarray
    """

    def __post_init__(self):
        # Ensure format is always "jpeg" by default for this class
        if not self.format or self.format == "raw":
            self.format = "jpeg"
        super().__post_init__()

    @classmethod
    def from_np_image(
        cls,
        image: "np.ndarray",           # HxW or HxWxC
        quality: int = 80,
        pixel_format: str = "BGR",     # how the input ndarray is laid out
    ):
        """
        Create an ImageFrameJpeg from a NumPy image.

        Args:
            image: np.ndarray in shape (H, W) or (H, W, C).
            quality: JPEG quality (typically 70-90).
            pixel_format: 'BGR', 'RGB' or 'GRAY' (must match the ndarray layout).
        """
        try:
            import numpy as np  # noqa: F401
            from simplejpeg import encode_jpeg
        except ImportError:
            print(
                "Could not import simplejpeg or numpy. "
                "Install with 'pip install simplejpeg numpy'.",
                file=sys.stderr,
            )
            raise

        if image.ndim == 2:
            height, width = image.shape
            channels = 1
            pixel_format = "GRAY"
        elif image.ndim == 3:
            height, width, channels = image.shape
            if channels not in (3, 4):
                raise ValueError(f"Unsupported channel count for JPEG: {channels}")
            # simplejpeg expects colorspace names like 'BGR', 'RGB'
        else:
            raise ValueError(f"Unsupported image shape for JPEG: {image.shape}")

        jpeg_bytes = encode_jpeg(
            image,
            quality=quality,
            colorspace=pixel_format.upper(),  # 'BGR', 'RGB', 'GRAY'
        )

        return cls(
            data=jpeg_bytes,
            format="jpeg",
            width=width,
            height=height,
            channels=channels,
            pixel_format=pixel_format.upper(),
        )

    def to_np_image(self, pixel_format: str = None):
        """
        Decode JPEG bytes back into a NumPy array.

        Args:
            pixel_format: Desired output colorspace ('BGR', 'RGB', 'GRAY').
                          If None, uses self.pixel_format when possible.

        Returns:
            np.ndarray with shape (H, W) or (H, W, C)
        """
        try:
            import numpy as np  # noqa: F401
            from simplejpeg import decode_jpeg
        except ImportError:
            Logger.error(
                "Could not import simplejpeg or numpy. "
                "Install with 'pip install simplejpeg numpy'."                
            )
            raise

        colorspace = (pixel_format or self.pixel_format or "BGR").upper()
        img = decode_jpeg(self.data, colorspace=colorspace)
        return img
