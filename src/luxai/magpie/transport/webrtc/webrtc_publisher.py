from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.utils.logger import Logger
from .webrtc_connection import WebRTCConnection


class WebRTCPublisher(StreamWriter):
    """
    WebRTC-based stream publisher.

    Writes frames or arbitrary data to the remote peer over a shared
    ``WebRTCConnection``.  The routing logic is fully internal:

    * ``ImageFrameRaw`` / ``ImageFrameCV`` / ``ImageFrameJpeg`` →
      native WebRTC video media track when negotiated with the remote peer,
      otherwise the ``"magpie-media"`` unreliable data channel fallback.
    * ``AudioFrameRaw`` / ``AudioFrameFlac`` →
      native WebRTC audio media track when negotiated, otherwise
      ``"magpie-media"`` fallback.
    * Everything else →
      ``"magpie"`` data channel (msgpack-serialized, topic-routed).

    Usage::

        conn = WebRTCConnection.with_mqtt("mqtt://broker:1883", session_id="my-robot")
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
        queue_size: int = 10,
    ):
        """
        Args:
            connection: Shared ``WebRTCConnection`` instance.
            queue_size: Size of the internal write-ahead queue.
        """
        self._connection = connection

        super().__init__(name="WebRTCPublisher", queue_size=queue_size)
        Logger.debug("WebRTCPublisher: ready.")

    # ------------------------------------------------------------------
    # StreamWriter implementation
    # ------------------------------------------------------------------

    _VIDEO_TOPIC = "video"
    _AUDIO_TOPIC = "audio"

    def _transport_write(self, data: object, topic: str):
        from luxai.magpie.frames.image import ImageFrameRaw
        from luxai.magpie.frames.audio import AudioFrameRaw

        use_media = self._connection._use_media_channels

        if isinstance(data, ImageFrameRaw):
            if use_media and topic and topic != self._VIDEO_TOPIC:
                Logger.warning(
                    f"WebRTCPublisher: topic='{topic}' is ignored for ImageFrameRaw "
                    f"when use_media_channels=True — frame goes to the single RTP video track. "
                    f"Use topic=VIDEO_TOPIC or set use_media_channels=False for topic routing."
                )
            self._write_video(data, topic)
        elif isinstance(data, AudioFrameRaw):
            if use_media and topic and topic != self._AUDIO_TOPIC:
                Logger.warning(
                    f"WebRTCPublisher: topic='{topic}' is ignored for AudioFrameRaw "
                    f"when use_media_channels=True — frame goes to the single RTP audio track. "
                    f"Use topic=AUDIO_TOPIC or set use_media_channels=False for topic routing."
                )
            self._write_audio(data, topic)
        else:
            if use_media and topic == self._VIDEO_TOPIC:
                Logger.warning(
                    f"WebRTCPublisher: topic=VIDEO_TOPIC ('{self._VIDEO_TOPIC}') is reserved for "
                    f"ImageFrameRaw when use_media_channels=True — data will be dropped. "
                    f"Use a custom topic string."
                )
                return
            if use_media and topic == self._AUDIO_TOPIC:
                Logger.warning(
                    f"WebRTCPublisher: topic=AUDIO_TOPIC ('{self._AUDIO_TOPIC}') is reserved for "
                    f"AudioFrameRaw when use_media_channels=True — data will be dropped. "
                    f"Use a custom topic string."
                )
                return
            self._write_data(data, topic)

    def _transport_close(self):
        # Connection is shared — closing the publisher does not disconnect.
        Logger.debug("WebRTCPublisher: closed.")

    # ------------------------------------------------------------------
    # Internal routing
    # ------------------------------------------------------------------

    def _write_video(self, frame: "ImageFrameRaw", topic: str):
        """Send an image frame: RTP track → magpie-media (unreliable) → magpie (reliable)."""
        track = self._connection.video_track
        if self._connection.video_negotiated and track is not None:
            # use_media_channels=True and RTP negotiated — fastest path
            try:
                av_frame = self._image_frame_to_av(frame)
                track.push(av_frame)
            except Exception as e:
                Logger.warning(f"WebRTCPublisher: video frame conversion failed: {e}")
        elif self._connection._use_media_channels:
            # use_media_channels=True but RTP not yet negotiated — unreliable DC fallback
            try:
                self._connection.send_media_frame(
                    {"kind": "video", "topic": topic or self._VIDEO_TOPIC, "payload": frame.to_dict()}
                )
            except Exception as e:
                Logger.warning(f"WebRTCPublisher: magpie-media video send failed: {e}")
        else:
            # use_media_channels=False — compress to JPEG then send through
            # the magpie data channel via the drop-stale queue.
            jpeg_frame = self._ensure_jpeg(frame)
            self._connection.enqueue_media_send({
                "type":    "media",
                "topic":   topic or self._VIDEO_TOPIC,
                "payload": jpeg_frame.to_dict(),
            })

    _audio_logged = False  # log audio frame properties once per publisher instance

    def _write_audio(self, frame: "AudioFrameRaw", topic: str):
        """Send an audio frame: RTP track → magpie-media (unreliable) → magpie (reliable)."""
        track = self._connection.audio_track
        if self._connection.audio_negotiated and track is not None:
            try:
                av_frame = self._audio_frame_to_av(frame)
                if not self.__class__._audio_logged:
                    self.__class__._audio_logged = True
                    Logger.debug(
                        f"WebRTCPublisher audio: in=({frame.sample_rate}Hz, "
                        f"{frame.channels}ch, {frame.bit_depth}bit, "
                        f"{len(frame.data)}bytes={len(frame.data)//(frame.channels*frame.bit_depth//8)}samples) "
                        f"→ av=({av_frame.sample_rate}Hz, "
                        f"{av_frame.samples}samples, fmt={av_frame.format.name})"
                    )
                track.push(av_frame)
            except Exception as e:
                Logger.warning(f"WebRTCPublisher: audio frame conversion failed: {e}")
        elif self._connection._use_media_channels:
            try:
                self._connection.send_media_frame(
                    {"kind": "audio", "topic": topic or self._AUDIO_TOPIC, "payload": frame.to_dict()}
                )
            except Exception as e:
                Logger.warning(f"WebRTCPublisher: magpie-media audio send failed: {e}")
        else:
            self._connection.enqueue_media_send({
                "type":    "media",
                "topic":   topic or self._AUDIO_TOPIC,
                "payload": frame.to_dict(),
            })

    def _write_data(self, data: object, topic: str):
        """Serialize and send arbitrary data over the data channel."""
        if not topic:
            Logger.warning("WebRTCPublisher: write() called without a topic — dropping.")
            return
        from luxai.magpie.frames.frame import Frame
        if isinstance(data, Frame):
            data = data.to_dict()
        self._connection.send_data({
            "type":    "pub",
            "topic":   topic,
            "payload": data,
        })

    # ------------------------------------------------------------------
    # Frame conversion helpers
    # ------------------------------------------------------------------

    def _ensure_jpeg(self, frame: "ImageFrameRaw") -> "ImageFrameRaw":
        """
        Return *frame* encoded as JPEG.

        - ``ImageFrameJpeg`` (or any frame whose format is already ``"jpeg"``)
          is returned unchanged — no re-encoding.
        - ``ImageFrameRaw`` with a raw/BGR payload is compressed at the quality
          configured via ``WebRTCOptions.media_channel_jpeg_quality``.
        """
        from luxai.magpie.frames.image import ImageFrameJpeg

        if isinstance(frame, ImageFrameJpeg) or (frame.format or "").lower() in ("jpeg", "jpg"):
            return frame

        import cv2
        import numpy as np

        fmt = (frame.format or "").lower()
        if fmt in ("raw", "") and frame.width and frame.height:
            arr = np.frombuffer(frame.data, dtype=np.uint8).reshape(
                frame.height, frame.width, frame.channels
            )
        else:
            raise ValueError(
                f"WebRTCPublisher: cannot JPEG-compress ImageFrame "
                f"with format='{frame.format}' — raw BGR/RGB required."
            )

        quality = self._connection._options.media_channel_jpeg_quality
        ok, buf = cv2.imencode(".jpg", arr, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if not ok:
            raise RuntimeError("WebRTCPublisher: cv2.imencode failed.")

        return ImageFrameJpeg(data=buf.tobytes(), width=frame.width, height=frame.height)

    @staticmethod
    def _image_frame_to_av(frame: "ImageFrameRaw") -> "av.VideoFrame":
        import av
        import numpy as np

        fmt = (frame.format or "").lower()

        if fmt in ("jpeg", ".jpg", ".jpeg"):
            # Decode directly to RGB — pixel_format describes the pre-encode source,
            # not the decoded output, so we request RGB from the decoder directly to
            # avoid a conditional flip that would be skipped for non-BGR sources
            # (e.g. RealSense sets pixel_format="RGB") causing a blue-tint artifact.
            try:
                from simplejpeg import decode_jpeg
                arr = decode_jpeg(frame.data, colorspace="RGB")
            except ImportError:
                import cv2
                arr = cv2.imdecode(
                    np.frombuffer(frame.data, np.uint8), cv2.IMREAD_COLOR
                )
                arr = arr[:, :, ::-1]   # cv2 gives BGR → RGB
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

        # Resample to 48000 Hz — Opus (used by aiortc) natively operates at
        # 48 kHz; feeding a different rate produces noise or distortion.
        # This is a no-op when sample_rate is already 48000.
        target_rate = 48000
        src_rate = frame.sample_rate
        if src_rate != target_rate:
            ratio = target_rate / src_rate
            out_len = int(round(len(samples) * ratio))
            x_old = np.linspace(0, 1, len(samples), endpoint=False)
            x_new = np.linspace(0, 1, out_len, endpoint=False)
            samples = np.interp(x_new, x_old, samples.astype(np.float64)).astype(dtype)

        # aiortc's opus encoder requires packed (interleaved) s16/s32 format,
        # not planar s16p/s32p.  Keep data interleaved and reshape to (1, N).
        if frame.channels == 2:
            num_samples = (len(samples) // 2) * 2   # truncate to even length
            samples = samples[:num_samples].reshape(1, -1)
            layout = "stereo"
        else:
            samples = samples.reshape(1, -1)
            layout = "mono"

        av_fmt = "s16" if frame.bit_depth == 16 else "s32"
        av_frame = av.AudioFrame.from_ndarray(samples, format=av_fmt, layout=layout)
        av_frame.sample_rate = target_rate
        return av_frame
