"""Frame identity assigned when native WebRTC media is decoded."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

pytest.importorskip("aiortc")
np = pytest.importorskip("numpy")

from aiortc.contrib.media import MediaStreamError  # noqa: E402
from luxai.magpie.transport.webrtc.webrtc_connection import (  # noqa: E402
    _PeerWebRTCConnection,
)


class _Track:
    def __init__(self, frames):
        self._frames = iter(frames)

    async def recv(self):
        try:
            return next(self._frames)
        except StopIteration:
            raise MediaStreamError from None


def test_decoded_media_frames_have_track_local_identity():
    receiver = object.__new__(_PeerWebRTCConnection)
    receiver._closing = False
    receiver._peer_id = "receiver"
    receiver._routing_lock = threading.RLock()
    video_frames = []
    audio_frames = []
    receiver._video_callbacks = {"camera": [lambda frame, topic: video_frames.append(frame)]}
    receiver._audio_callbacks = {"microphone": [lambda frame, topic: audio_frames.append(frame)]}

    pixels = np.zeros((2, 2, 3), dtype=np.uint8)
    video_frame = SimpleNamespace(to_ndarray=lambda format: pixels)
    asyncio.run(receiver._receive_video(_Track([video_frame] * 3), "camera"))
    first_gid = video_frames[0].gid
    assert [frame.id for frame in video_frames] == [0, 1, 2]
    assert {frame.gid for frame in video_frames} == {first_gid}

    # A replacement track starts its own identity and sequence.
    asyncio.run(receiver._receive_video(_Track([video_frame]), "camera"))
    assert video_frames[-1].gid != first_gid
    assert video_frames[-1].id == 0

    samples = np.zeros((1, 960), dtype=np.int16)
    audio_frame = SimpleNamespace(
        layout=SimpleNamespace(channels=[object()], name="mono"),
        sample_rate=48000,
        to_ndarray=lambda: samples,
    )
    asyncio.run(receiver._receive_audio(_Track([audio_frame] * 2), "microphone"))
    assert [frame.id for frame in audio_frames] == [0, 1]
    assert len({frame.gid for frame in audio_frames}) == 1
    assert audio_frames[0].gid not in {frame.gid for frame in video_frames}
