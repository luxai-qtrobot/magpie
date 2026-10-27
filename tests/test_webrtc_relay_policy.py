import asyncio
from types import SimpleNamespace

from luxai.magpie.transport.webrtc.webrtc_connection import (
    _filter_remote_sdp_for_relay,
    _gather_relay_only_candidates,
    _is_public_ice_address,
)


def test_public_ice_address_rejects_private_and_mdns_candidates():
    assert _is_public_ice_address("8.8.8.8")
    assert _is_public_ice_address("2001:4860:4860::8888")
    assert not _is_public_ice_address("10.0.0.4")
    assert not _is_public_ice_address("192.168.1.20")
    assert not _is_public_ice_address("peer.local")


def test_relay_remote_sdp_keeps_public_candidates_only():
    sdp = (
        "v=0\r\n"
        "a=candidate:1 1 udp 1 192.168.1.20 5000 typ host\r\n"
        "a=candidate:2 1 udp 1 8.8.8.8 5001 typ srflx\r\n"
        "a=candidate:3 1 udp 1 48.209.57.190 49160 typ relay\r\n"
        "a=end-of-candidates\r\n"
    )

    filtered = _filter_remote_sdp_for_relay(sdp)

    assert "192.168.1.20" not in filtered
    assert "8.8.8.8" in filtered
    assert "48.209.57.190" in filtered
    assert "a=end-of-candidates" in filtered


def test_gather_relay_only_candidates_prunes_aiortc_candidate_list():
    host = SimpleNamespace(type="host")
    srflx = SimpleNamespace(type="srflx")
    relay = SimpleNamespace(type="relay")

    class Gatherer:
        def __init__(self):
            self.gathered = False
            self._connection = SimpleNamespace(
                _local_candidates=[host, srflx, relay]
            )

        async def gather(self):
            self.gathered = True

    gatherer = Gatherer()
    ice_transport = SimpleNamespace(iceGatherer=gatherer)
    dtls_transport = SimpleNamespace(transport=ice_transport)
    transceiver = SimpleNamespace(
        receiver=SimpleNamespace(transport=dtls_transport)
    )
    pc = SimpleNamespace(
        getTransceivers=lambda: [transceiver],
        sctp=None,
    )

    asyncio.run(_gather_relay_only_candidates(pc))

    assert gatherer.gathered
    assert gatherer._connection._local_candidates == [relay]
