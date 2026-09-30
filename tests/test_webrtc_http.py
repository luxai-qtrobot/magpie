"""HTTP signaling adapters, wire contract, and a real aiortc connection."""

import asyncio
import base64
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import pytest

httpx = pytest.importorskip("httpx")

from examples.webrtc.http_signaling import (  # noqa: E402
    InMemoryRelay, SignalingASGI, SignalingHTTP, SignalingWSGI,
)
from examples.webrtc.http_signaling.relay import HTTPResult  # noqa: E402
from luxai.magpie.transport.webrtc import (  # noqa: E402
    HttpSignaler,
    WebRTCConnection,
    WebRTCOptions,
    WebRtcStreamReader,
    WebRtcStreamWriter,
    WebRTCRpcRequester,
    WebRTCRpcResponder,
)
from luxai.magpie.tools._webrtc_tools_common import (  # noqa: E402
    build_signaler, http_headers_type,
)


@pytest.fixture
def relay():
    class Server(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    class Handler(WSGIRequestHandler):
        def log_message(self, *_args):
            pass

    signaling = SignalingWSGI(SignalingHTTP(InMemoryRelay()))

    def app(environ, start_response):
        if environ.get("HTTP_AUTHORIZATION") != "Bearer test-token":
            start_response("401 Unauthorized", [("Content-Length", "0")])
            return [b""]
        return signaling(environ, start_response)

    server = make_server(
        "127.0.0.1", 0, app, server_class=Server, handler_class=Handler
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/signal"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _auth():
    return {"Authorization": "Bearer test-token"}


def _segment(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def test_join_announcements_are_cached_and_exchanged_atomically():
    protocol = SignalingHTTP(InMemoryRelay())

    def peer_path(peer):
        return f"/sessions/{_segment('room')}/peers/{_segment(peer)}"

    def join(peer, announcement):
        result = protocol.handle("PUT", peer_path(peer), body=announcement)
        assert result.status == 204
        assert result.headers["X-Magpie-Join-Announcements"] == "1"

    def inbox(peer, after=0):
        return protocol.handle(
            "GET", peer_path(peer) + "/messages", query=f"after={after}&wait=0"
        )

    join("a", b"hello-a")
    assert inbox("a").status == 204  # no self-echo
    join("b", b"hello-b")
    assert inbox("a").body == b"hello-b"
    assert inbox("b").body == b"hello-a"

    # Both peers are already connected when c joins. The relay replays their
    # cached hellos to c and announces c to both existing peers.
    join("c", b"hello-c")
    assert inbox("c").body == b"hello-a"
    assert inbox("c", after=1).body == b"hello-b"
    assert inbox("a", after=1).body == b"hello-c"
    assert inbox("b", after=1).body == b"hello-c"

    join("b", b"hello-b")
    assert inbox("a", after=2).status == 204  # repeated PUT is idempotent

    # A legacy client may register without a body, then supply one later.
    join("d", b"")
    assert inbox("d").body == b"hello-a"
    join("d", b"hello-d")
    assert inbox("a", after=2).body == b"hello-d"


def test_cli_http_signaling_with_auth_headers(relay, tmp_path):
    headers_file = tmp_path / "headers.json"
    headers_file.write_text('{"Authorization":"Bearer test-token"}', encoding="utf-8")
    headers = http_headers_type(f"@{headers_file}")
    first = build_signaler(relay, "cli-room", timeout=2, http_headers=headers)
    second = build_signaler(relay, "cli-room", timeout=2, http_headers=headers)
    try:
        assert isinstance(first, HttpSignaler)
        assert isinstance(second, HttpSignaler)
        assert first.participant_id != second.participant_id
    finally:
        first.disconnect()
        second.disconnect()


def test_asgi_adapter_exposes_complete_protocol():
    app = SignalingASGI(SignalingHTTP(InMemoryRelay()))

    async def exercise():
        lifecycle = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
        lifecycle_responses = []

        async def receive_lifecycle():
            return next(lifecycle)

        async def send_lifecycle(message):
            lifecycle_responses.append(message["type"])

        await app({"type": "lifespan"}, receive_lifecycle, send_lifecycle)
        assert lifecycle_responses == [
            "lifespan.startup.complete", "lifespan.shutdown.complete"
        ]

        transport = httpx.ASGITransport(app=app, root_path="/signal")
        async with httpx.AsyncClient(
            transport=transport, base_url="http://relay"
        ) as client:
            a = f"/signal/sessions/{_segment('room')}/peers/{_segment('a')}"
            b = f"/signal/sessions/{_segment('room')}/peers/{_segment('b')}"
            assert (await client.put(a)).status_code == 204
            assert (await client.put(b)).status_code == 204
            third = await client.put(f"/signal/sessions/{_segment('room')}/peers/cw")
            assert third.status_code == 204

            headers = {"X-Magpie-Message-Id": "message-1"}
            sent = await client.post(a + "/messages", content=b"opaque", headers=headers)
            assert sent.status_code == 204
            received = await client.get(b + "/messages", params={"after": 0, "wait": 0})
            assert received.status_code == 200
            assert received.content == b"opaque"
            assert received.headers["X-Magpie-Sequence"] == "1"
            duplicate = await client.post(a + "/messages", content=b"opaque", headers=headers)
            assert duplicate.status_code == 204
            empty = await client.get(b + "/messages", params={"after": 1, "wait": 0})
            assert empty.status_code == 204
            waiting = asyncio.create_task(
                client.get(b + "/messages", params={"after": 1, "wait": 1})
            )
            await asyncio.sleep(0.05)
            assert not waiting.done()
            await client.post(
                a + "/messages", content=b"next",
                headers={"X-Magpie-Message-Id": "message-2"},
            )
            next_message = await waiting
            assert next_message.content == b"next"
            assert next_message.headers["X-Magpie-Sequence"] == "2"
            invalid = await client.get(b + "/messages", params={"wait": "nan"})
            assert invalid.status_code == 400
            assert (await client.delete(b)).status_code == 204
            gone = await client.get(b + "/messages", params={"wait": 0})
            assert gone.status_code == 404

            # FastAPI mounts this ASGI adapter, so its PUT body must survive
            # the adapter and be replayed to a peer that joins later.
            x = f"/signal/sessions/{_segment('cached')}/peers/{_segment('x')}"
            y = f"/signal/sessions/{_segment('cached')}/peers/{_segment('y')}"
            joined = await client.put(x, content=b"hello-x")
            assert joined.headers["X-Magpie-Join-Announcements"] == "1"
            assert (await client.put(y, content=b"hello-y")).status_code == 204
            x_message = await client.get(x + "/messages", params={"wait": 0})
            y_message = await client.get(y + "/messages", params={"wait": 0})
            assert x_message.content == b"hello-y"
            assert y_message.content == b"hello-x"

    asyncio.run(exercise())


def test_http_signaler_delivery_auth_retry_and_borrowed_client(relay):
    calls = []

    def credentials():
        calls.append(1)
        return _auth()

    client = httpx.Client()
    a = HttpSignaler(
        relay, "room with / slash",
        headers={"Authorization": "Bearer expired"},
        headers_provider=credentials,
        http_client=client, poll_wait=0.2,
    )
    b = HttpSignaler(relay, "room with / slash", headers=_auth(), poll_wait=0.2)
    received = []
    event = threading.Event()
    b.subscribe(lambda payload: (received.append(payload), event.set()))
    try:
        a.publish(b"offer")
        assert event.wait(3)
        assert received == [b"offer"]

        # Retrying a POST with the same ID must not deliver another offer.
        url = (
            f"{relay}/sessions/{_segment('room with / slash')}/"
            f"peers/{_segment(a.participant_id)}/messages"
        )
        headers = {**_auth(), "X-Magpie-Message-Id": "same-id"}
        assert client.post(url, content=b"answer", headers=headers).status_code == 204
        assert client.post(url, content=b"answer", headers=headers).status_code == 204
        deadline = time.monotonic() + 3
        while len(received) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert received == [b"offer", b"answer"]
        assert len(calls) >= 2  # provider used for join and send
    finally:
        a.disconnect()
        b.disconnect()
        assert not client.is_closed  # injected client belongs to caller
        client.close()


def test_http_signaler_unsubscribe_replays_pending_message(relay):
    a = HttpSignaler(relay, "replay", headers=_auth(), poll_wait=0.2)
    b = HttpSignaler(relay, "replay", headers=_auth(), poll_wait=0.2)
    received = []
    event = threading.Event()
    try:
        a.subscribe(lambda payload: received.append(payload))
        a.unsubscribe()
        b.publish(b"pending")
        time.sleep(0.3)
        assert received == []
        a.subscribe(lambda payload: (received.append(payload), event.set()))
        assert event.wait(3)
        assert received == [b"pending"]
    finally:
        a.disconnect()
        b.disconnect()


def test_http_signaler_rejects_bad_authorization(relay):
    with pytest.raises(httpx.HTTPStatusError):
        HttpSignaler(relay, "auth", headers={"Authorization": "Bearer wrong"})


def test_http_signaler_accepts_third_participant(relay):
    a = HttpSignaler(relay, "full", headers=_auth())
    b = HttpSignaler(relay, "full", headers=_auth())
    c = None
    try:
        c = HttpSignaler(relay, "full", headers=_auth())
        assert len({a.participant_id, b.participant_id, c.participant_id}) == 3
    finally:
        a.disconnect()
        b.disconnect()
        if c:
            c.disconnect()


def test_http_signaler_rejoins_after_mailbox_expires(relay):
    a = HttpSignaler(relay, "renew", headers=_auth(), poll_wait=0.2)
    b = HttpSignaler(relay, "renew", headers=_auth(), poll_wait=0.2)
    received = []
    first = threading.Event()
    second = threading.Event()

    def on_message(payload):
        received.append(payload)
        (first if payload == b"first" else second).set()

    b.subscribe(on_message)
    try:
        a.publish(b"first")
        assert first.wait(3)

        # Simulate relay expiry, including its message sequence resetting to 1.
        peer_url = f"{relay}/sessions/{_segment('renew')}/peers/{_segment(b.participant_id)}"
        with httpx.Client(headers=_auth()) as client:
            assert client.delete(peer_url).status_code == 204
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                response = client.get(peer_url + "/messages", params={"after": 0, "wait": 0})
                if response.status_code == 204:
                    break
                time.sleep(0.02)
            else:
                pytest.fail("signaler did not rejoin after mailbox expiry")

        a.publish(b"second")
        assert second.wait(3)
        assert received == [b"first", b"second"]
    finally:
        a.disconnect()
        b.disconnect()


def test_older_relay_without_cached_join_header_uses_hello_posts(relay, monkeypatch):
    pytest.importorskip("aiortc")
    original_handle = SignalingHTTP.handle
    original_send = InMemoryRelay.send
    sent = []

    def without_capability(self, method, path, query="", headers=None, body=b""):
        response = original_handle(self, method, path, query, headers, body)
        if method == "PUT":
            return HTTPResult(response.status, response.body)
        return response

    def count_send(self, session, peer, message_id, payload):
        sent.append(payload)
        return original_send(self, session, peer, message_id, payload)

    monkeypatch.setattr(SignalingHTTP, "handle", without_capability)
    monkeypatch.setattr(InMemoryRelay, "send", count_send)
    connection = WebRTCConnection.with_http(
        relay, "older-relay", headers=_auth(), poll_wait=0.2,
        options=WebRTCOptions(stun_servers=[]),
    )
    try:
        assert not connection.connect(timeout=1.3)
        assert not connection._signaler.supports_join_announcements
        assert sent  # legacy relay needs periodic POSTs for discovery
    finally:
        connection.disconnect()


def test_http_signaling_webrtc_stream_rpc_and_media(relay, monkeypatch):
    pytest.importorskip("aiortc")
    from luxai.magpie.frames.image import ImageFrameRaw

    sent = []
    original_send = InMemoryRelay.send

    def count_send(self, session, peer, message_id, payload):
        sent.append((session, peer, payload))
        return original_send(self, session, peer, message_id, payload)

    monkeypatch.setattr(InMemoryRelay, "send", count_send)
    options = WebRTCOptions(stun_servers=[], video_topics=["/camera"])
    left = WebRTCConnection.with_http(
        relay, "rtc", headers=_auth(), poll_wait=0.2, options=options
    )
    right = None
    data_reader = None
    video_reader = None
    writer = None
    requester = None
    responder = None
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            lfuture = executor.submit(left.connect, 15)
            # The second peer can join after the first has started connecting.
            time.sleep(1.2)
            assert sent == []  # no periodic hello POST while alone
            right = WebRTCConnection.with_http(
                relay, "rtc", headers=_auth(), poll_wait=0.2, options=options
            )
            data_reader = WebRtcStreamReader(right, topic="demo")
            video_reader = WebRtcStreamReader(right, topic="/camera")
            rfuture = executor.submit(right.connect, 15)
            assert lfuture.result(timeout=20)
            assert rfuture.result(timeout=20)
        posts_after_connect = len(sent)
        time.sleep(1.3)
        assert len(sent) == posts_after_connect  # healthy peers stay quiet
        deadline = time.monotonic() + 5
        while (not left.is_video_negotiated("/camera")
               or not right.is_video_negotiated("/camera")) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert left.is_video_negotiated("/camera")
        assert right.is_video_negotiated("/camera")

        writer = WebRtcStreamWriter(left, queue_size=0)
        writer.write({"hello": 7}, topic="demo")
        assert data_reader.read(timeout=5) == ({"hello": 7}, "demo")

        responder = WebRTCRpcResponder(right, service_name="echo")
        requester = WebRTCRpcRequester(left, service_name="echo")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                responder.handle_once, handler=lambda request: {"echo": request}, timeout=5
            )
            assert requester.call({"value": 2}, timeout=5) == {"echo": {"value": 2}}
            future.result(timeout=7)

        frame = ImageFrameRaw(
            data=bytes(64 * 64 * 3), width=64, height=64,
            channels=3, pixel_format="BGR",
        )
        for _ in range(8):
            writer.write(frame, topic="/camera")
            time.sleep(0.05)
        received, topic = video_reader.read(timeout=10)
        assert isinstance(received, ImageFrameRaw)
        assert topic == "/camera"
        assert (received.width, received.height) == (64, 64)
    finally:
        if requester:
            requester.close()
        if responder:
            responder.close()
        if writer:
            writer.close()
        if data_reader:
            data_reader.close()
        if video_reader:
            video_reader.close()
        left.disconnect()
        if right:
            right.disconnect()


def test_http_webrtc_late_joiner_fanout_and_rpc_reply_routing(relay):
    pytest.importorskip("aiortc")
    from luxai.magpie.frames.image import ImageFrameRaw
    session = "three-peers"
    connections = [
        WebRTCConnection.with_http(
            relay, session, headers=_auth(), poll_wait=0.2,
            options=WebRTCOptions(stun_servers=[], video_topics=["/camera"]),
            role=role,
        ) for role in ("host", "client", "client")
    ]
    hub, first, late = connections
    readers = []
    writer = None
    responder = None
    requesters = []
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            a = executor.submit(hub.connect, 15)
            b = executor.submit(first.connect, 15)
            assert a.result(timeout=20)
            assert b.result(timeout=20)

            first_reader = WebRtcStreamReader(first, topic="events")
            readers.append(first_reader)
            writer = WebRtcStreamWriter(hub, queue_size=0)
            writer.write({"seq": 1}, topic="events")
            assert first_reader.read(timeout=5) == ({"seq": 1}, "events")

            c = executor.submit(late.connect, 15)
            assert c.result(timeout=20)
            deadline = time.monotonic() + 10
            while len(hub.peer_ids) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            assert len(hub.peer_ids) == 2
            assert len(first.peer_ids) == 1
            assert len(late.peer_ids) == 1

            late_reader = WebRtcStreamReader(late, topic="events")
            readers.append(late_reader)
            writer.write({"seq": 2}, topic="events")
            assert first_reader.read(timeout=5) == ({"seq": 2}, "events")
            assert late_reader.read(timeout=5) == ({"seq": 2}, "events")

            video_readers = [
                WebRtcStreamReader(first, topic="/camera"),
                WebRtcStreamReader(late, topic="/camera"),
            ]
            readers.extend(video_readers)
            frame = ImageFrameRaw(
                data=bytes(64 * 64 * 3), width=64, height=64,
                channels=3, pixel_format="BGR",
            )
            for _ in range(8):
                writer.write(frame, topic="/camera")
                time.sleep(0.05)
            for video_reader in video_readers:
                received, topic = video_reader.read(timeout=10)
                assert isinstance(received, ImageFrameRaw)
                assert topic == "/camera"

            responder = WebRTCRpcResponder(hub, service_name="echo")
            requesters = [
                WebRTCRpcRequester(first, service_name="echo"),
                WebRTCRpcRequester(late, service_name="echo"),
            ]
            for requester, number in zip(requesters, (1, 2)):
                response = executor.submit(
                    responder.handle_once,
                    handler=lambda request: {"reply": request}, timeout=5,
                )
                assert requester.call({"from": number}, timeout=5) == {
                    "reply": {"from": number}
                }
                response.result(timeout=7)
            assert not hub._rpc_origins
    finally:
        for requester in requesters:
            requester.close()
        if responder:
            responder.close()
        if writer:
            writer.close()
        for reader in readers:
            reader.close()
        for connection in connections:
            connection.disconnect()


@pytest.mark.parametrize("host_reconnect,first_reconnect", [
    (True, False), (False, True), (True, True),
])
def test_clients_join_before_host_and_one_link_recovers(
    relay, host_reconnect, first_reconnect,
):
    pytest.importorskip("aiortc")
    session = "clients-first"
    options = WebRTCOptions(stun_servers=[])
    host, first, second = [
        WebRTCConnection.with_http(
            relay, session, headers=_auth(), poll_wait=0.2,
            options=options, role=role, reconnect=reconnect,
        ) for role, reconnect in (
            ("host", host_reconnect),
            ("client", first_reconnect),
            ("client", False),
        )
    ]
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            first_result = executor.submit(first.connect, 20)
            second_result = executor.submit(second.connect, 20)
            deadline = time.monotonic() + 5
            while (not first._signaler.supports_join_announcements
                   or not second._signaler.supports_join_announcements):
                assert time.monotonic() < deadline
                time.sleep(0.02)
            assert not first.peer_ids and not second.peer_ids

            host_result = executor.submit(host.connect, 20)
            assert host_result.result(timeout=25)
            assert first_result.result(timeout=25)
            assert second_result.result(timeout=25)
            deadline = time.monotonic() + 10
            while len(host.peer_ids) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            assert set(host.peer_ids) == {first.peer_id, second.peer_id}
            healthy_peer = host._peers[second.peer_id]
            broken_peer = host._peers[first.peer_id]

            host._loop.call_soon_threadsafe(broken_peer._data_channel.close)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if (set(host.peer_ids) == {first.peer_id, second.peer_id}
                        and first.peer_ids == [host.peer_id]
                        and host._peers[first.peer_id] is not broken_peer):
                    break
                time.sleep(0.05)
            else:
                pytest.fail("the dropped host-client link did not recover")
            assert host._peers[second.peer_id] is healthy_peer
    finally:
        for connection in (host, first, second):
            connection.disconnect()
