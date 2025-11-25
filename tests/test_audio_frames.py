import unittest
import io
import struct

from luxai.magpie.frames import AudioFrameRaw, AudioFrameFlac


class TestAudioFrameRaw(unittest.TestCase):

    def test_raw_initialization_with_bytes(self):
        data = b'\x01\x02\x03\x04'
        f = AudioFrameRaw(
            channels=1,
            sample_rate=16000,
            bit_depth=16,
            data=data
        )

        self.assertEqual(f.data, data)
        self.assertEqual(f.num_frames, len(data) // 2)  # 16-bit → 2 bytes per frame

    def test_raw_initialization_with_list(self):
        data_list = [1, 2, 3, 4]
        f = AudioFrameRaw(
            channels=1,
            sample_rate=16000,
            bit_depth=8,
            data=data_list
        )

        # list must be normalized to bytes
        self.assertIsInstance(f.data, bytes)
        self.assertEqual(f.data, bytes(data_list))
        self.assertEqual(f.num_frames, len(data_list))  # 8-bit → 1 byte per frame

    def test_raw_multichannel_frame_count(self):
        # 2 channels, 16-bit → 4 bytes per frame
        pcm = b'\x01\x02\x03\x04\x05\x06\x07\x08'  # 2 frames
        f = AudioFrameRaw(channels=2, bit_depth=16, data=pcm)
        self.assertEqual(f.num_frames, 2)

    def test_raw_str(self):
        pcm = b'\x01\x02'
        f = AudioFrameRaw(channels=1, sample_rate=16000, data=pcm)
        s = str(f)
        self.assertIn("AudioFrameRaw", s)
        self.assertIn("size: 2", s)
        self.assertIn("frames: 1", s)


class TestAudioFrameFlac(unittest.TestCase):

    def setUp(self):
        # Check dependencies
        try:
            import soundfile  # noqa
            import numpy  # noqa
        except ImportError:
            self.skipTest("soundfile and numpy required for FLAC tests")

    def _generate_pcm(self, frames=10, channels=1, sample_rate=16000):
        """
        Generate simple PCM wave of incrementing samples.
        """
        import numpy as np

        samples = np.arange(frames * channels, dtype=np.int16)
        if channels > 1:
            samples = samples.reshape(frames, channels)

        return samples.tobytes()

    def test_flac_from_pcm_single_channel(self):
        pcm = self._generate_pcm(frames=20, channels=1)
        f = AudioFrameFlac.from_pcm(
            pcm_bytes=pcm,
            channels=1,
            sample_rate=16000,
            bit_depth=16
        )

        self.assertEqual(f.channels, 1)
        self.assertEqual(f.sample_rate, 16000)
        self.assertEqual(f.bit_depth, 16)
        self.assertEqual(f.format, "FLAC")
        self.assertIsInstance(f.data, bytes)
        self.assertGreater(len(f.data), 0)

    def test_flac_roundtrip(self):
        pcm = self._generate_pcm(frames=50, channels=2, sample_rate=22050)

        f = AudioFrameFlac.from_pcm(
            pcm_bytes=pcm,
            channels=2,
            sample_rate=22050,
            bit_depth=16
        )

        pcm_out = f.to_pcm()

        # Round-trip must preserve PCM bytes exactly
        self.assertEqual(pcm, pcm_out)

        # Metadata must be updated properly after decoding
        self.assertEqual(f.channels, 2)
        self.assertEqual(f.sample_rate, 22050)
        self.assertEqual(f.bit_depth, 16)

if __name__ == "__main__":
    unittest.main()
