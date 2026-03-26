from queue import Queue, Empty
from typing import Optional, Tuple

from luxai.magpie.transport.stream_reader import StreamReader
from luxai.magpie.utils.logger import Logger
from .webrtc_connection import WebRTCConnection


class WebRTCSubscriber(StreamReader):
    """
    WebRTC-based stream subscriber.

    Receives frames or data published by the remote peer over a shared
    ``WebRTCConnection``.  The *topic* parameter controls routing:

    * ``"video"`` (or the special sentinel ``WebRTCSubscriber.VIDEO_TOPIC``) →
      subscribes to the video media track; each ``read()`` returns an
      ``ImageFrameRaw`` with raw BGR pixel data.
    * ``"audio"`` (or ``WebRTCSubscriber.AUDIO_TOPIC``) →
      subscribes to the audio media track; each ``read()`` returns an
      ``AudioFrameRaw`` with PCM data.
    * Any other topic →
      subscribes to that topic on the ``"magpie"`` data channel; ``read()``
      returns the deserialized payload.

    Usage::

        conn = WebRTCConnection(signaling=signal_conn)
        conn.connect()

        # General data
        sub = WebRTCSubscriber(conn, topic="robot/state")
        data, topic = sub.read(timeout=5.0)

        # Video frames
        vsub = WebRTCSubscriber(conn, topic="video")
        frame, _ = vsub.read(timeout=5.0)   # frame is ImageFrameRaw

        sub.close()
        vsub.close()
    """

    VIDEO_TOPIC = "video"
    AUDIO_TOPIC = "audio"

    def __init__(
        self,
        connection: WebRTCConnection,
        topic: str,
        queue_size: int = 10,
    ):
        """
        Args:
            connection: Shared ``WebRTCConnection`` instance.
            topic: Topic to subscribe to, or ``"video"`` / ``"audio"`` for
                   media tracks.
            queue_size: Size of the internal reader queue.
        """
        self._connection = connection
        self._topic = topic
        self._msg_queue: Queue = Queue()

        # Register with the connection before starting the StreamReader thread
        if topic == self.VIDEO_TOPIC:
            self._connection.add_video_callback(self._on_media_frame)
        elif topic == self.AUDIO_TOPIC:
            self._connection.add_audio_callback(self._on_media_frame)
        else:
            self._connection.add_pub_callback(topic, self._on_data_message)

        super().__init__(name="WebRTCSubscriber", queue_size=queue_size)
        Logger.debug(f"WebRTCSubscriber: subscribed to '{topic}'.")

    # ------------------------------------------------------------------
    # Internal callbacks (called from WebRTCConnection routing)
    # ------------------------------------------------------------------

    def _on_data_message(self, payload: object, topic: str):
        """Invoked by WebRTCConnection for each matching data channel message."""
        self._msg_queue.put_nowait((payload, topic))

    def _on_media_frame(self, frame: object):
        """Invoked by WebRTCConnection for each received media frame."""
        self._msg_queue.put_nowait((frame, self._topic))

    # ------------------------------------------------------------------
    # StreamReader implementation
    # ------------------------------------------------------------------

    def _transport_read_blocking(self, timeout: Optional[float] = None) -> Tuple[object, str]:
        try:
            return self._msg_queue.get(timeout=timeout)
        except Empty:
            raise TimeoutError(
                f"WebRTCSubscriber: no data received"
                + (f" within {timeout}s" if timeout is not None else "")
            )

    def _transport_close(self):
        if self._topic == self.VIDEO_TOPIC:
            self._connection.remove_video_callback(self._on_media_frame)
        elif self._topic == self.AUDIO_TOPIC:
            self._connection.remove_audio_callback(self._on_media_frame)
        else:
            self._connection.remove_pub_callback(self._topic, self._on_data_message)
        Logger.debug(f"WebRTCSubscriber: unsubscribed from '{self._topic}'.")
