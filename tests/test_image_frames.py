import unittest
import sys
from luxai.magpie.frames import (
    ImageFrameRaw,
    ImageFrameCV,
    ImageFrameJpeg,
)


# ==========================================================
#  TEST: ImageFrameRaw
# ==========================================================

class TestImageFrameRaw(unittest.TestCase):

    def test_raw_init_basic(self):
        data = b"\x01\x02\x03\x04"
        f = ImageFrameRaw(
            data=data,
            format="raw",
            width=10,
            height=5,
            channels=3,
            pixel_format="RGB",
        )

        self.assertEqual(f.data, data)
        self.assertEqual(f.format, "raw")
        self.assertEqual(f.width, 10)
        self.assertEqual(f.height, 5)
        self.assertEqual(f.channels, 3)
        self.assertEqual(f.pixel_format, "RGB")

    def test_raw_str(self):
        f = ImageFrameRaw(
            data=b"\x00" * 10,
            width=3,
            height=3,
            channels=3,
            format="raw"
        )
        s = str(f)
        self.assertIn("ImageFrameRaw", s)
        self.assertIn("size: 10", s)
        self.assertIn("3x3x3", s)


# ==========================================================
#  TEST: ImageFrameCV  (requires cv2 + numpy)
# ==========================================================

class TestImageFrameCV(unittest.TestCase):

    def setUp(self):
        try:
            import cv2   # noqa
            import numpy # noqa
        except ImportError:
            self.skipTest("opencv-python and numpy are required")

    def test_from_cv_image_and_back(self):
        import cv2
        import numpy as np

        # Create simple BGR test image
        img = np.zeros((10, 20, 3), dtype=np.uint8)
        img[:] = (10, 100, 200)

        f = ImageFrameCV.from_cv_image(img, format=".jpg")

        # Basic metadata checks
        self.assertEqual(f.width, 20)
        self.assertEqual(f.height, 10)
        self.assertEqual(f.channels, 3)
        self.assertEqual(f.pixel_format, "BGR")
        self.assertTrue(len(f.data) > 0)

        decoded = f.to_cv_image()
        self.assertEqual(decoded.shape, (10, 20, 3))

    def test_from_cv_image_gray(self):
        import cv2
        import numpy as np

        img = np.full((5, 7), 128, dtype=np.uint8)  # grayscale

        f = ImageFrameCV.from_cv_image(img)
        self.assertEqual(f.channels, 1)
        self.assertEqual(f.pixel_format, "GRAY")
        self.assertEqual(f.width, 7)
        self.assertEqual(f.height, 5)


# ==========================================================
#  TEST: ImageFrameJpeg  (requires simplejpeg + numpy)
# ==========================================================

class TestImageFrameJpeg(unittest.TestCase):

    def setUp(self):
        try:
            import numpy  # noqa
            from simplejpeg import encode_jpeg  # noqa
        except ImportError:
            self.skipTest("numpy and simplejpeg required")

    def test_from_np_image_and_back(self):
        import numpy as np

        img = np.zeros((10, 15, 3), dtype=np.uint8)
        img[:] = (50, 150, 200)

        f = ImageFrameJpeg.from_np_image(img, quality=80, pixel_format="BGR")

        # Metadata must reflect the ndarray shape
        self.assertEqual(f.width, 15)
        self.assertEqual(f.height, 10)
        self.assertEqual(f.channels, 3)
        self.assertEqual(f.format, "jpeg")
        self.assertEqual(f.pixel_format, "BGR")

        # JPEG bytes must not be empty
        self.assertTrue(len(f.data) > 0)

        # Round-trip check (colorspace preserved)
        img2 = f.to_np_image()
        self.assertEqual(img2.shape, img.shape)


    def test_gray_image(self):
        import numpy as np

        img = np.full((6, 8), 200, dtype=np.uint8)  # grayscale

        f = ImageFrameJpeg.from_np_image(img)

        # Metadata must reflect grayscale layout
        self.assertEqual(f.channels, 1)
        self.assertEqual(f.pixel_format, "GRAY")
        self.assertEqual(f.width, 8)
        self.assertEqual(f.height, 6)

        # Decode back into GRAY format
        decoded = f.to_np_image(pixel_format="GRAY")

        # simplejpeg returns (H, W, 1) for grayscale
        self.assertEqual(decoded.shape, (6, 8, 1))

        # Compare image content
        self.assertTrue(np.array_equal(decoded[:, :, 0], img))



if __name__ == "__main__":
    unittest.main()

