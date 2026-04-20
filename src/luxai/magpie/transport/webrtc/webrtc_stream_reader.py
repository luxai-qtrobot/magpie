from queue import Queue, Empty
from typing import Optional, Tuple

from luxai.magpie.transport.stream_reader import StreamReader
from luxai.magpie.utils.logger import Logger
from .webrtc_connection import WebRTCConnection


class WebRtcStreamReader(StreamReader):
    """
    WebRTC-based stream reader.

    Receives frames or data published by the remote peer over a shared
    ``WebRTCConnection``.  The *topic* parameter controls routing:

    **use_media_channels=True** (default):

    * If *topic* is in ``connection.video_topics`` → RTP video track;
      ``read()`` returns ``ImageFrameRaw``.
    * If *topic* is in ``connection.audio_topics`` → RTP audio track;
      ``read()`` returns ``AudioFrameRaw``.
    * Any other topic → data channel streaming.

    **use_media_channels=False**:

    * Any topic → data channel streaming; video/audio frames are topic-routed
      via the ``magpie-media`` unreliable data channel.

    Usage::

        conn = WebRTCConnection.with_mqtt(
            "mqtt://broker:1883", session_id="my-robot",
            options=WebRTCOptions(video_topics=["/camera/color/image"]),
        )
        conn.connect()

        # Video frames (RTP track)
        vsub = WebRtcStreamReader(conn, topic="/camera/color/image")
        frame, _ = vsub.read(timeout=5.0)   # ImageFrameRaw

        # General data
        sub = WebRtcStreamReader(conn, topic="robot/state")
        data, topic = sub.read(timeout=5.0)

        vsub.close()
        sub.close()
    """

    def __init__(
        self,
        connection: WebRTCConnection,
        topic: str,
        queue_size: int = 10,
    ):
        """
        Args:
            connection: Shared ``WebRTCConnection`` instance.
            topic: Topic to subscribe to.
            queue_size: Size of the internal reader queue.
        """
        self._connection = connection
        self._topic = topic
        self._msg_queue: Queue = Queue()

        use_media = connection._use_media_channels

        if use_media and topic in connection.video_topics:
            self._connection.add_video_callback(topic, self._on_media_frame)
            self._kind = "video"
        elif use_media and topic in connection.audio_topics:
            self._connection.add_audio_callback(topic, self._on_media_frame)
            self._kind = "audio"
        else:
            if use_media and topic:
                # Topic not declared in options — route as data channel streaming.
                # This is expected for non-media topics; only warn if it looks
                # like the user intended a media topic but forgot to declare it.
                pass
            self._connection.add_pub_callback(topic, self._on_data_message)
            self._kind = "data"

        super().__init__(name="WebRtcStreamReader", queue_size=queue_size)
        Logger.debug(f"WebRtcStreamReader: subscribed to '{topic}' (kind={self._kind}).")

    # ------------------------------------------------------------------
    # Internal callbacks (called from WebRTCConnection routing)
    # ------------------------------------------------------------------

    def _on_data_message(self, payload: object, topic: str):
        """Invoked by WebRTCConnection for each matching data channel message."""
        self._msg_queue.put_nowait((payload, topic))

    def _on_media_frame(self, frame: object, topic: str):
        """Invoked by WebRTCConnection for each received media frame."""
        self._msg_queue.put_nowait((frame, topic))

    # ------------------------------------------------------------------
    # StreamReader implementation
    # ------------------------------------------------------------------

    def _transport_read_blocking(self, timeout: Optional[float] = None) -> Tuple[object, str]:
        try:
            return self._msg_queue.get(timeout=timeout)
        except Empty:
            raise TimeoutError(
                f"WebRtcStreamReader: no data received"
                + (f" within {timeout}s" if timeout is not None else "")
            )

    def _transport_close(self):
        if self._kind == "video":
            self._connection.remove_video_callback(self._topic, self._on_media_frame)
        elif self._kind == "audio":
            self._connection.remove_audio_callback(self._topic, self._on_media_frame)
        else:
            self._connection.remove_pub_callback(self._topic, self._on_data_message)
        Logger.debug(f"WebRtcStreamReader: unsubscribed from '{self._topic}'.")
