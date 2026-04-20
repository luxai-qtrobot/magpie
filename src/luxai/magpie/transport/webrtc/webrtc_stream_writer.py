from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.utils.logger import Logger
from .webrtc_connection import WebRTCConnection


class WebRtcStreamWriter(StreamWriter):
    """
    WebRTC-based stream writer.

    Writes frames or arbitrary data to the remote peer over a shared
    ``WebRTCConnection``.  Routing:

    * ``ImageFrameRaw`` / ``ImageFrameCV`` / ``ImageFrameJpeg`` →
      native WebRTC video RTP track when the topic is in
      ``connection.video_topics`` and was negotiated; otherwise the
      ``"magpie-media"`` unreliable data-channel fallback (or the reliable
      ``"magpie"`` channel when ``use_media_channels=False``).
    * ``AudioFrameRaw`` →
      native WebRTC audio RTP track when the topic is in
      ``connection.audio_topics`` and was negotiated; otherwise fallback.
    * Everything else →
      ``"magpie"`` data channel (msgpack-serialized, topic-routed).

    Usage::

        conn = WebRTCConnection.with_mqtt(
            "mqtt://broker:1883", session_id="my-robot",
            options=WebRTCOptions(video_topics=["/camera/color/image"]),
        )
        conn.connect()

        pub = WebRtcStreamWriter(conn)
        pub.write(frame, topic="/camera/color/image")   # → RTP video track
        pub.write({"speed": 1.0}, topic="robot/cmd")    # → data channel
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

        super().__init__(name="WebRtcStreamWriter", queue_size=queue_size)
        Logger.debug("WebRtcStreamWriter: ready.")

    # ------------------------------------------------------------------
    # StreamWriter implementation
    # ------------------------------------------------------------------

    def _transport_write(self, data: object, topic: str):
        from luxai.magpie.frames.image import ImageFrameRaw
        from luxai.magpie.frames.audio import AudioFrameRaw

        if isinstance(data, ImageFrameRaw):
            self._write_video(data, topic)
        elif isinstance(data, AudioFrameRaw):
            self._write_audio(data, topic)
        else:
            self._write_data(data, topic)

    def _transport_close(self):
        # Connection is shared — closing the writer does not disconnect.
        Logger.debug("WebRtcStreamWriter: closed.")

    # ------------------------------------------------------------------
    # Internal routing
    # ------------------------------------------------------------------

    def _write_video(self, frame: "ImageFrameRaw", topic: str):
        """Send an image frame: RTP track (if topic declared) → magpie-media → magpie."""
        conn = self._connection
        use_media = conn._use_media_channels
        in_topics = topic in conn.video_topics if use_media else False

        if use_media and in_topics and conn.is_video_negotiated(topic):
            # RTP path — fastest
            track = conn.get_video_track(topic)
            try:
                av_frame = self._image_frame_to_av(frame)
                track.push(av_frame)
            except Exception as e:
                Logger.warning(f"WebRtcStreamWriter: video RTP conversion failed for '{topic}': {e}")
        elif use_media:
            if not in_topics and topic:
                Logger.warning(
                    f"WebRtcStreamWriter: topic='{topic}' is not in video_topics — "
                    "falling back to magpie-media data channel."
                )
            try:
                conn.send_media_frame(
                    {"kind": "video", "topic": topic, "payload": frame.to_dict()}
                )
            except Exception as e:
                Logger.warning(f"WebRtcStreamWriter: magpie-media video send failed: {e}")
        else:
            # use_media_channels=False — compress to JPEG, send via drop-stale queue
            jpeg_frame = self._ensure_jpeg(frame)
            conn.enqueue_media_send({
                "type":    "media",
                "topic":   topic,
                "payload": jpeg_frame.to_dict(),
            })

    _OPUS_FRAME_SIZE = 960   # samples @ 48000 Hz = 20 ms, the standard Opus frame size
    _audio_logged = False   # log audio frame properties once per writer instance

    def _write_audio(self, frame: "AudioFrameRaw", topic: str):
        """Send an audio frame: RTP track (if topic declared) → magpie-media → magpie."""
        conn = self._connection
        use_media = conn._use_media_channels
        in_topics = topic in conn.audio_topics if use_media else False

        if use_media and in_topics and conn.is_audio_negotiated(topic):
            track = conn.get_audio_track(topic)
            try:
                av_frame = self._audio_frame_to_av(frame)
                if not self.__class__._audio_logged:
                    self.__class__._audio_logged = True
                    Logger.debug(
                        f"WebRtcStreamWriter audio: in=({frame.sample_rate}Hz, "
                        f"{frame.channels}ch, {frame.bit_depth}bit, "
                        f"{len(frame.data)}bytes={len(frame.data)//(frame.channels*frame.bit_depth//8)}samples) "
                        f"→ av=({av_frame.sample_rate}Hz, "
                        f"{av_frame.samples}samples, fmt={av_frame.format.name})"
                    )
                # Opus requires exactly 960 samples @ 48 kHz (20 ms) per frame.
                # Buffer samples and push only complete frames to avoid distortion.
                import av
                import numpy as np
                new_samples = av_frame.to_ndarray().flatten()
                buf_key = f"_audio_buf_{topic}"
                layout_key = f"_audio_buf_layout_{topic}"
                if not hasattr(self, buf_key):
                    setattr(self, buf_key, np.array([], dtype=np.int16))
                    setattr(self, layout_key, av_frame.layout.name)
                buf = np.concatenate([getattr(self, buf_key), new_samples])
                setattr(self, buf_key, buf)
                while len(buf) >= self._OPUS_FRAME_SIZE:
                    chunk = buf[:self._OPUS_FRAME_SIZE]
                    buf = buf[self._OPUS_FRAME_SIZE:]
                    setattr(self, buf_key, buf)
                    out = av.AudioFrame.from_ndarray(
                        chunk.reshape(1, -1), format="s16", layout=getattr(self, layout_key)
                    )
                    out.sample_rate = self._OPUS_FRAME_SIZE * 50  # 960 * 50 = 48000
                    track.push(out)
            except Exception as e:
                Logger.warning(f"WebRtcStreamWriter: audio RTP conversion failed for '{topic}': {e}")
        elif use_media:
            if not in_topics and topic:
                Logger.warning(
                    f"WebRtcStreamWriter: topic='{topic}' is not in audio_topics — "
                    "falling back to magpie-media data channel."
                )
            try:
                conn.send_media_frame(
                    {"kind": "audio", "topic": topic, "payload": frame.to_dict()}
                )
            except Exception as e:
                Logger.warning(f"WebRtcStreamWriter: magpie-media audio send failed: {e}")
        else:
            conn.enqueue_media_send({
                "type":    "media",
                "topic":   topic,
                "payload": frame.to_dict(),
            })

    def _write_data(self, data: object, topic: str):
        """Serialize and send arbitrary data over the data channel."""
        if not topic:
            Logger.warning("WebRtcStreamWriter: write() called without a topic — dropping.")
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
                f"WebRtcStreamWriter: cannot JPEG-compress ImageFrame "
                f"with format='{frame.format}' — raw BGR/RGB required."
            )

        quality = self._connection._options.media_channel_jpeg_quality
        ok, buf = cv2.imencode(".jpg", arr, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if not ok:
            raise RuntimeError("WebRtcStreamWriter: cv2.imencode failed.")

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
                f"WebRtcStreamWriter: cannot convert ImageFrame "
                f"format='{frame.format}' to av.VideoFrame"
            )

        if arr.ndim == 3 and arr.shape[2] == 4:
            raise ValueError(
                f"WebRtcStreamWriter: 4-channel frames (RGBA/BGRA) are not supported. "
                "Convert to 3-channel (RGB/BGR) before publishing."
            )

        # Single-channel (grayscale) → replicate to RGB
        if arr.ndim == 2 or (arr.ndim == 3 and arr.shape[2] == 1):
            arr = np.repeat(arr.reshape(arr.shape[0], arr.shape[1], 1), 3, axis=2)

        # av expects RGB; our frames are typically BGR — convert
        elif getattr(frame, "pixel_format", "BGR").upper().startswith("BGR"):
            arr = arr[:, :, ::-1].copy()

        return av.VideoFrame.from_ndarray(arr, format="rgb24")

    @staticmethod
    def _audio_frame_to_av(frame: "AudioFrameRaw") -> "av.AudioFrame":
        import av
        import numpy as np

        if frame.format != "PCM":
            raise ValueError(
                f"WebRtcStreamWriter: cannot convert AudioFrame "
                f"format='{frame.format}' — only PCM is supported"
            )

        if frame.bit_depth not in (16, 32):
            raise ValueError(
                f"WebRtcStreamWriter: unsupported bit_depth={frame.bit_depth} — only 16 and 32 are supported"
            )

        if frame.channels > 2:
            raise ValueError(
                f"WebRtcStreamWriter: unsupported channels={frame.channels} — only mono (1) and stereo (2) are supported"
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
