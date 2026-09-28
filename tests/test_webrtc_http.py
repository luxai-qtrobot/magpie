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
from luxai.magpie.transport.webrtc import (  # noqa: E402
    HttpSignaler,
    WebRTCConnection,
    WebRTCOptions,
    WebRtcStreamReader,
    WebRtcStreamWriter,
    WebRTCRpcRequester,
    WebRTCRpcResponder,
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
            assert third.status_code == 409

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


def test_http_signaler_rejects_third_participant(relay):
    a = HttpSignaler(relay, "full", headers=_auth())
    b = HttpSignaler(relay, "full", headers=_auth())
    try:
        with pytest.raises(httpx.HTTPStatusError) as error:
            HttpSignaler(relay, "full", headers=_auth())
        assert error.value.response.status_code == 409
        assert "already has two participants" in str(error.value)
    finally:
        a.disconnect()
        b.disconnect()


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


def test_http_signaling_webrtc_stream_rpc_and_media(relay):
    pytest.importorskip("aiortc")
    from luxai.magpie.frames.image import ImageFrameRaw

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
            right = WebRTCConnection.with_http(
                relay, "rtc", headers=_auth(), poll_wait=0.2, options=options
            )
            data_reader = WebRtcStreamReader(right, topic="demo")
            video_reader = WebRtcStreamReader(right, topic="/camera")
            rfuture = executor.submit(right.connect, 15)
            assert lfuture.result(timeout=20)
            assert rfuture.result(timeout=20)
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
