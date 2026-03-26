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
        conn = WebRTCConnection(signaling=signal_conn, options=opts)
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
