"""Shared setup for the deployable WebRTC smoke-test peers."""

from __future__ import annotations

import json
from urllib.parse import quote
from urllib.request import Request, urlopen

from luxai.magpie.transport.webrtc import WebRTCOptions, WebRTCTurnServer


def fetch_webrtc_options(server_url: str, session_id: str, force_turn: bool):
    ice_url = f"{server_url.rstrip('/')}/ice/{quote(session_id, safe='')}"
    request = Request(ice_url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=10) as response:
        config = json.load(response)

    return WebRTCOptions(
        stun_servers=config["stunServers"],
        turn_servers=[
            WebRTCTurnServer(
                url=server["url"],
                username=server["username"],
                credential=server["credential"],
            )
            for server in config["turnServers"]
        ],
        ice_transport_policy="relay" if force_turn else "all",
    )
