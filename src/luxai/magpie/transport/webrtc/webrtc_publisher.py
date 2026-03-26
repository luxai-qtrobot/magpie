from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.utils.logger import Logger
from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from .webrtc_connection import WebRTCConnection


class WebRTCPublisher(StreamWriter):
    """
    WebRTC-based stream publisher.

    Writes frames or arbitrary data to the remote peer over a shared
    ``WebRTCConnection``.  The routing logic is fully internal:

    * ``ImageFrameRaw`` / ``ImageFrameCV`` / ``ImageFrameJpeg`` →
      native WebRTC video media track (H.264 / VP8, no msgpack overhead).
    * ``AudioFrameRaw`` / ``AudioFrameFlac`` →
      native WebRTC audio media track (Opus / PCMU).
    * Everything else →
      ``"magpie"`` data channel (msgpack-serialized, topic-routed).

    Usage::

        conn = WebRTCConnection(signaling=signal_conn)
        conn.connect()

        pub = WebRTCPublisher(conn)

        # General data (data channel)
        pub.write({"motor": [0.1, 0.2, 0.3]}, topic="robot/state")

        # Video frame (media track) — topic ignored for media
        frame = ImageFrameCV.from_cv_image(cv_image)
        pub.write(frame, topic="robot/camera")

        pub.close()
    """

    def __init__(
        self,
        connection: WebRTCConnection,
        serializer=None,
        queue_size: int = 10,
    ):
        """
        Args:
            connection: Shared ``WebRTCConnection`` instance.
            serializer: Serializer for data channel messages.
                        Defaults to ``MsgpackSerializer``.
            queue_size: Size of the internal write-ahead queue.
        """
        self._connection = connection
        self._serializer = serializer or MsgpackSerializer()

        super().__init__(name="WebRTCPublisher", queue_size=queue_size)
        Logger.debug("WebRTCPublisher: ready.")

    # ------------------------------------------------------------------
    # StreamWriter implementation
    # ------------------------------------------------------------------

    def _transport_write(self, data: object, topic: str):
        from luxai.magpie.frames.image import ImageFrameRaw
        from luxai.magpie.frames.audio import AudioFrameRaw

        if isinstance(data, ImageFrameRaw):
            self._write_video(data)
        elif isinstance(data, AudioFrameRaw):
            self._write_audio(data)
        else:
            self._write_data(data, topic)

    def _transport_close(self):
        # Connection is shared — closing the publisher does not disconnect.
        Logger.debug("WebRTCPublisher: closed.")

    # ------------------------------------------------------------------
    # Internal routing
    # ------------------------------------------------------------------

    def _write_video(self, frame: "ImageFrameRaw"):
        """Convert ImageFrameRaw → av.VideoFrame and push to the video track."""
        track = self._connection.video_track
        if track is None:
            Logger.warning("WebRTCPublisher: video track not ready — dropping frame.")
            return
        try:
            av_frame = self._image_frame_to_av(frame)
            track.push(av_frame)
        except Exception as e:
            Logger.warning(f"WebRTCPublisher: video frame conversion failed: {e}")

    def _write_audio(self, frame: "AudioFrameRaw"):
        """Convert AudioFrameRaw → av.AudioFrame and push to the audio track."""
        track = self._connection.audio_track
        if track is None:
            Logger.warning("WebRTCPublisher: audio track not ready — dropping frame.")
            return
        try:
            av_frame = self._audio_frame_to_av(frame)
            track.push(av_frame)
        except Exception as e:
            Logger.warning(f"WebRTCPublisher: audio frame conversion failed: {e}")

    def _write_data(self, data: object, topic: str):
        """Serialize and send arbitrary data over the data channel."""
        if not topic:
            Logger.warning("WebRTCPublisher: write() called without a topic — dropping.")
            return
        self._connection.send_data({
            "type":    "pub",
            "topic":   topic,
            "payload": data,
        })

    # ------------------------------------------------------------------
    # Frame conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _image_frame_to_av(frame: "ImageFrameRaw") -> "av.VideoFrame":
        import av
        import numpy as np

        fmt = (frame.format or "").lower()

        if fmt in ("jpeg", ".jpg", ".jpeg"):
            # Prefer simplejpeg; fall back to OpenCV
            try:
                from simplejpeg import decode_jpeg
                arr = decode_jpeg(frame.data, colorspace="BGR")
            except ImportError:
                import cv2
                arr = cv2.imdecode(
                    np.frombuffer(frame.data, np.uint8), cv2.IMREAD_COLOR
                )
        elif fmt in ("raw", "") and frame.width and frame.height:
            arr = np.frombuffer(frame.data, dtype=np.uint8).reshape(
                frame.height, frame.width, frame.channels
            )
        else:
            raise ValueError(
                f"WebRTCPublisher: cannot convert ImageFrame "
                f"format='{frame.format}' to av.VideoFrame"
            )

        if arr.ndim == 3 and arr.shape[2] == 4:
            raise ValueError(
                f"WebRTCPublisher: 4-channel frames (RGBA/BGRA) are not supported. "
                "Convert to 3-channel (RGB/BGR) before publishing."
            )

        # av expects RGB; our frames are typically BGR — convert
        if getattr(frame, "pixel_format", "BGR") == "BGR" and arr.ndim == 3:
            arr = arr[:, :, ::-1].copy()

        return av.VideoFrame.from_ndarray(arr, format="rgb24")

    @staticmethod
    def _audio_frame_to_av(frame: "AudioFrameRaw") -> "av.AudioFrame":
        import av
        import numpy as np

        if frame.format != "PCM":
            raise ValueError(
                f"WebRTCPublisher: cannot convert AudioFrame "
                f"format='{frame.format}' — only PCM is supported"
            )

        if frame.bit_depth not in (16, 32):
            raise ValueError(
                f"WebRTCPublisher: unsupported bit_depth={frame.bit_depth} — only 16 and 32 are supported"
            )

        if frame.channels > 2:
            raise ValueError(
                f"WebRTCPublisher: unsupported channels={frame.channels} — only mono (1) and stereo (2) are supported"
            )

        dtype = np.int16 if frame.bit_depth == 16 else np.int32
        samples = np.frombuffer(frame.data, dtype=dtype)

        if frame.channels == 2:
            # Interleaved PCM → planar (channels, samples)
            num_samples = len(samples) // 2
            samples = samples[: num_samples * 2].reshape(num_samples, 2).T
            layout = "stereo"
            av_fmt = "s16p" if frame.bit_depth == 16 else "s32p"
        else:
            layout = "mono"
            av_fmt = "s16" if frame.bit_depth == 16 else "s32"

        av_frame = av.AudioFrame.from_ndarray(samples, format=av_fmt, layout=layout)
        av_frame.sample_rate = frame.sample_rate
        return av_frame
