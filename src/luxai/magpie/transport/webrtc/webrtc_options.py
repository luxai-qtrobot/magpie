from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class WebRTCTurnServer:
    """TURN relay server configuration."""
    url: str                            # e.g. "turn:myturn.server:3478"
    username: Optional[str] = None
    credential: Optional[str] = None


@dataclass
class WebRTCOptions:
    """
    WebRTC connection options.

    Pass an instance of this to ``WebRTCConnection`` to configure ICE servers,
    codec preferences, data channel behaviour, and session identification.

    Example::

        opts = WebRTCOptions(
            stun_servers=["stun:stun.l.google.com:19302"],
            turn_servers=[
                WebRTCTurnServer(
                    url="turn:myturn.server:3478",
                    username="user",
                    credential="pass",
                )
            ],
            video_codec="H264",
            video_bitrate=2000,
        )
        conn = WebRTCConnection.with_mqtt("mqtt://broker:1883", "my-robot", options=opts)
    """

    # ---- ICE / NAT traversal -------------------------------------------
    stun_servers: List[str] = field(
        default_factory=lambda: ["stun:stun.l.google.com:19302"]
    )
    """List of STUN server URIs.  Google's public server is used by default."""

    turn_servers: List[WebRTCTurnServer] = field(default_factory=list)
    """Optional TURN relay servers for strict NAT / corporate firewall
    scenarios.  Leave empty to rely on STUN only (covers ~85 % of cases)."""

    ice_transport_policy: str = "all"
    """ICE candidate policy:
    - ``"all"``   — try direct, STUN-reflexive, and TURN relay candidates.
    - ``"relay"`` — force TURN relay only (useful for testing or strict security)."""

    # ---- Data channel ---------------------------------------------------
    data_channel_ordered: bool = True
    """Whether the data channel delivers messages in order (default ``True``)."""

    data_channel_max_retransmits: Optional[int] = None
    """Maximum retransmit count for unreliable channels.
    ``None`` = fully reliable (default).  ``0`` = fire-and-forget."""

    # ---- Codec preferences ---------------------------------------------
    video_codec: str = "H264"
    """Preferred video codec for media tracks: ``"H264"``, ``"VP8"``, or ``"VP9"``."""

    audio_codec: str = "opus"
    """Preferred audio codec for media tracks: ``"opus"`` or ``"PCMU"``."""

    video_bitrate: int = 2000
    """Target video bitrate in kbps (default 2000)."""

    audio_bitrate: int = 96
    """Target audio bitrate in kbps (default 96)."""

    use_media_channels: bool = True
    """If ``True`` (default), use native WebRTC media tracks for
    ``ImageFrameRaw`` / ``AudioFrameRaw`` when the remote peer supports them.
    If ``False``, always use the data channel fallback regardless of remote
    capabilities."""

    audio_topics: List[str] = field(default_factory=list)
    """List of audio topic paths that should be transmitted as native RTP audio
    tracks (one track per topic).  Requires ``use_media_channels=True``.
    The order determines the transceiver order in the SDP offer/answer.

    Example::

        options = WebRTCOptions(
            audio_topics=["/mic/int/audio/ch0/stream:o", "/media/audio/fg/stream:i"]
        )
    """

    video_topics: List[str] = field(default_factory=list)
    """List of video topic paths that should be transmitted as native RTP video
    tracks (one track per topic).  Requires ``use_media_channels=True``.

    Example::

        options = WebRTCOptions(
            video_topics=["/camera/color/image"]
        )
    """

    media_channel_jpeg_quality: int = 80
    """JPEG quality (1-100) used to compress ``ImageFrameRaw`` frames before
    sending over the data channel when ``use_media_channels=False``.
    Lower values reduce bandwidth at the cost of image quality.
    ``ImageFrameJpeg`` frames are forwarded as-is without re-encoding.
    Default is 80, which gives a good quality/size trade-off for robot video."""
