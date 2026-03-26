"""
WebRTCConnection — shared WebRTC peer connection for MAGPIE.

Architecture overview
---------------------
* One ``WebRTCConnection`` per peer pair, shared by all publishers,
  subscribers, and RPC components — mirroring ``MqttConnection``.
* A single ``asyncio`` event loop runs in a dedicated background thread;
  all aiortc operations live there.
* Signaling (SDP offer/answer + ICE candidates) is exchanged via any
  MAGPIE-compatible signaling object that exposes ``publish()`` and
  ``add_subscription()`` — a plain ``MqttConnection`` works out of the box.
* Role (offer vs answer) is auto-negotiated: both peers broadcast a
  ``hello`` message; the peer with the lexicographically higher ``peer_id``
  creates the SDP offer.
* Media routing on the single ``"magpie"`` data channel uses a lightweight
  envelope: ``{"type": "pub"|"rpc_req"|"rpc_ack"|"rpc_rep", ...}``.
* Video and audio ``ImageFrame*`` / ``AudioFrame*`` are streamed over
  dedicated ``RTCVideoTrack`` / ``RTCAudioTrack`` for native codec support.
"""

import asyncio
import threading
from queue import Queue
from typing import Callable, Dict, List, Optional

from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from .webrtc_options import WebRTCOptions

try:
    from aiortc import (
        RTCPeerConnection,
        RTCSessionDescription,
        RTCIceServer,
        RTCConfiguration,
        MediaStreamTrack,
    )
    from aiortc.contrib.media import MediaStreamError
    import av
    _AIORTC_AVAILABLE = True
except ImportError:
    _AIORTC_AVAILABLE = False
    MediaStreamTrack = object  # fallback base for type checking


# ---------------------------------------------------------------------------
# Internal media tracks (created inside the asyncio loop)
# ---------------------------------------------------------------------------

class _MagpieVideoTrack(MediaStreamTrack):
    """
    Custom aiortc video source track backed by a thread-safe push queue.

    Frames are pushed from any thread via ``push(av_frame)``.
    aiortc calls ``recv()`` on the asyncio loop to pull the next frame.
    """
    kind = "video"

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False

    def push(self, av_frame: "av.VideoFrame") -> None:
        """Thread-safe: enqueue an av.VideoFrame for transmission."""
        if not self._closed:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, av_frame)

    async def recv(self) -> "av.VideoFrame":
        pts, time_base = await self.next_timestamp()
        while not self._closed:
            try:
                frame = self._queue.get_nowait()
                frame.pts = pts
                frame.time_base = time_base
                return frame
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.005)
        raise MediaStreamError()

    def stop(self):
        self._closed = True
        super().stop()


class _MagpieAudioTrack(MediaStreamTrack):
    """
    Custom aiortc audio source track backed by a thread-safe push queue.

    Frames are pushed from any thread via ``push(av_frame)``.
    """
    kind = "audio"

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False

    def push(self, av_frame: "av.AudioFrame") -> None:
        """Thread-safe: enqueue an av.AudioFrame for transmission."""
        if not self._closed:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, av_frame)

    async def recv(self) -> "av.AudioFrame":
        pts, time_base = await self.next_timestamp()
        while not self._closed:
            try:
                frame = self._queue.get_nowait()
                frame.pts = pts
                frame.time_base = time_base
                return frame
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.005)
        raise MediaStreamError()

    def stop(self):
        self._closed = True
        super().stop()


# ---------------------------------------------------------------------------
# WebRTCConnection
# ---------------------------------------------------------------------------

class WebRTCConnection:
    """
    Shared WebRTC peer connection.

    Create **one** instance per peer pair and pass it to
    ``WebRTCPublisher``, ``WebRTCSubscriber``, ``WebRTCRpcRequester``, and
    ``WebRTCRpcResponder``.

    The *signaling* parameter accepts any object that exposes
    ``publish(topic, payload_bytes)`` and
    ``add_subscription(topic, callback)`` / ``remove_subscription(topic, callback)``
    — a plain ``MqttConnection`` satisfies this interface.

    Usage::

        signal_conn = MqttConnection("mqtt://broker.hivemq.com:1883")
        signal_conn.connect()

        conn = WebRTCConnection(signaling=signal_conn)
        if not conn.connect(timeout=20.0):
            raise RuntimeError("WebRTC handshake timed out")

        pub = WebRTCPublisher(conn)
        sub = WebRTCSubscriber(conn, topic="robot/state")

        # ... use pub / sub ...

        pub.close()
        sub.close()
        conn.disconnect()
        signal_conn.disconnect()
    """

    def __init__(self, signaling, session_id: str, options: Optional[WebRTCOptions] = None):
        if not _AIORTC_AVAILABLE:
            raise ImportError(
                "aiortc is required for WebRTC transport. "
                "Install with: pip install 'luxai-magpie[webrtc]'"
            )

        self._signaling = signaling
        self._options = options or WebRTCOptions()
        self._serializer = MsgpackSerializer()

        # Session / peer identity
        self._session_id: str = session_id
        self._peer_id: str = get_uinque_id()[:12]
        self._signal_topic: str = f"magpie/webrtc/{self._session_id}/signal"

        # aiortc objects (created in asyncio loop)
        self._pc: Optional["RTCPeerConnection"] = None
        self._data_channel = None
        self._video_track: Optional[_MagpieVideoTrack] = None
        self._audio_track: Optional[_MagpieAudioTrack] = None

        # Asyncio loop in background thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

        # Signaling state
        self._remote_peer_id: Optional[str] = None
        self._role_decided = False
        self._pending_ice_candidates: list = []

        # Message routing (thread-safe)
        self._routing_lock = threading.Lock()
        self._pub_callbacks: Dict[str, List[Callable]] = {}       # topic  → [callbacks]
        self._rpc_service_queues: Dict[str, Queue] = {}            # service → Queue
        self._rpc_reply_callbacks: Dict[str, Callable] = {}        # rid    → callback

        # Incoming video / audio frame callbacks
        self._video_callbacks: List[Callable] = []
        self._audio_callbacks: List[Callable] = []

        # Connection state
        self._connected = False
        self._connect_success = False
        self._connect_event = threading.Event()
        self._closing = False

        Logger.debug(
            f"WebRTCConnection: peer_id={self._peer_id}, "
            f"session_id={self._session_id}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def peer_id(self) -> str:
        return self._peer_id

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self, timeout: Optional[float] = None) -> bool:
        """
        Initiate the WebRTC handshake and block until the peer connection is
        established or *timeout* seconds elapse.

        Args:
            timeout: Maximum seconds to wait for the connection to be established.
                     ``None`` (default) waits indefinitely — matching the pub/sub
                     philosophy where a peer may appear at any time.

        Returns ``True`` on success, ``False`` on timeout or failure.
        """
        self._connect_event.clear()
        self._connect_success = False

        # Start dedicated asyncio loop in a background daemon thread
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop, name="WebRTCLoop", daemon=True
        )
        self._loop_thread.start()

        # Subscribe to signaling channel
        self._signaling.add_subscription(self._signal_topic, self._on_signal_message)

        # Kick off the async setup
        asyncio.run_coroutine_threadsafe(self._connect_async(), self._loop)

        connected = self._connect_event.wait(timeout=timeout)
        if not connected:
            Logger.warning(
                f"WebRTCConnection({self._peer_id}): "
                f"connect timed out after {timeout}s"
            )
        return self._connect_success

    def disconnect(self):
        """Close the peer connection and clean up resources."""
        self._closing = True
        self._connected = False

        self._signaling.remove_subscription(self._signal_topic, self._on_signal_message)

        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._close_async(), self._loop).result(timeout=5.0)
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=3.0)

        Logger.debug(f"WebRTCConnection({self._peer_id}): disconnected.")

    # ------------------------------------------------------------------
    # Registration API (used by publisher / subscriber / rpc classes)
    # ------------------------------------------------------------------

    def add_pub_callback(self, topic: str, callback: Callable) -> None:
        """Register a callback for incoming pub messages on *topic*."""
        with self._routing_lock:
            self._pub_callbacks.setdefault(topic, []).append(callback)

    def remove_pub_callback(self, topic: str, callback: Callable) -> None:
        with self._routing_lock:
            callbacks = self._pub_callbacks.get(topic, [])
            try:
                callbacks.remove(callback)
            except ValueError:
                pass

    def add_rpc_service(self, service: str, queue: Queue) -> None:
        """Register an incoming-request queue for an RPC service."""
        with self._routing_lock:
            self._rpc_service_queues[service] = queue

    def remove_rpc_service(self, service: str) -> None:
        with self._routing_lock:
            self._rpc_service_queues.pop(service, None)

    def register_rpc_reply(self, rid: str, callback: Callable) -> None:
        """Register a callback for the ACK/reply of an in-flight RPC call."""
        with self._routing_lock:
            self._rpc_reply_callbacks[rid] = callback

    def unregister_rpc_reply(self, rid: str) -> None:
        with self._routing_lock:
            self._rpc_reply_callbacks.pop(rid, None)

    def add_video_callback(self, callback: Callable) -> None:
        """Register a callback for incoming video frames (ImageFrameRaw)."""
        with self._routing_lock:
            self._video_callbacks.append(callback)

    def remove_video_callback(self, callback: Callable) -> None:
        with self._routing_lock:
            try:
                self._video_callbacks.remove(callback)
            except ValueError:
                pass

    def add_audio_callback(self, callback: Callable) -> None:
        """Register a callback for incoming audio frames (AudioFrameRaw)."""
        with self._routing_lock:
            self._audio_callbacks.append(callback)

    def remove_audio_callback(self, callback: Callable) -> None:
        with self._routing_lock:
            try:
                self._audio_callbacks.remove(callback)
            except ValueError:
                pass

    @property
    def video_track(self) -> Optional[_MagpieVideoTrack]:
        """The outbound video track (for pushing frames from WebRTCPublisher)."""
        return self._video_track

    @property
    def audio_track(self) -> Optional[_MagpieAudioTrack]:
        """The outbound audio track (for pushing frames from WebRTCPublisher)."""
        return self._audio_track

    def send_data(self, msg: dict) -> None:
        """
        Thread-safe: serialize *msg* and send it on the data channel.

        Silently drops the message if the channel is not yet open.
        """
        if self._closing or self._data_channel is None or self._data_channel.readyState != "open":
            return
        try:
            payload = self._serializer.serialize(msg)
            self._loop.call_soon_threadsafe(self._data_channel.send, payload)
        except Exception as e:
            Logger.warning(f"WebRTCConnection({self._peer_id}): send_data failed: {e}")

    # ------------------------------------------------------------------
    # Internal: asyncio loop
    # ------------------------------------------------------------------

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _close_async(self):
        if self._video_track:
            self._video_track.stop()
        if self._audio_track:
            self._audio_track.stop()
        if self._pc:
            await self._pc.close()

    # ------------------------------------------------------------------
    # Internal: connection setup
    # ------------------------------------------------------------------

    async def _connect_async(self):
        # Build ICE server list from options
        ice_servers = []
        for stun_uri in self._options.stun_servers:
            ice_servers.append(RTCIceServer(urls=stun_uri))
        for turn in self._options.turn_servers:
            ice_servers.append(
                RTCIceServer(
                    urls=turn.url,
                    username=turn.username,
                    credential=turn.credential,
                )
            )

        config = RTCConfiguration(iceServers=ice_servers)
        self._pc = RTCPeerConnection(configuration=config)

        # Create outbound media tracks (always included so SDP is symmetric)
        self._video_track = _MagpieVideoTrack(self._loop)
        self._audio_track = _MagpieAudioTrack(self._loop)

        # Wire up RTCPeerConnection event handlers
        self._setup_pc_handlers()

        # Start the hello retry loop (runs concurrently in asyncio)
        asyncio.ensure_future(self._hello_loop())

    async def _hello_loop(self):
        """
        Broadcast a ``hello`` every second until the remote peer responds.
        Role negotiation happens in ``_handle_signal`` on receipt of the
        remote peer's ``hello``.
        """
        for _ in range(30):  # up to 30 seconds
            if self._closing:
                return
            self._send_signal({"type": "hello", "peer_id": self._peer_id})
            await asyncio.sleep(1.0)
            if self._remote_peer_id is not None:
                return

        Logger.warning(
            f"WebRTCConnection({self._peer_id}): "
            "no remote peer found — check that both peers use the same session_id."
        )
        self._connect_event.set()  # unblock connect() with failure

    def _setup_pc_handlers(self):
        pc = self._pc

        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = pc.connectionState
            Logger.debug(f"WebRTCConnection({self._peer_id}): state → {state}")
            if state == "connected":
                self._connected = True
                self._connect_success = True
                self._connect_event.set()
            elif state in ("failed", "closed"):
                self._connected = False
                if not self._connect_event.is_set():
                    self._connect_event.set()  # unblock connect() with failure

        @pc.on("icecandidate")
        async def on_icecandidate(candidate):
            if candidate:
                self._send_signal({
                    "type":         "candidate",
                    "peer_id":      self._peer_id,
                    "candidate":    candidate.candidate,
                    "sdpMid":       candidate.sdpMid,
                    "sdpMLineIndex": candidate.sdpMLineIndex,
                })

        @pc.on("datachannel")
        def on_datachannel(channel):
            # Answer side receives the data channel created by the offerer
            if channel.label == "magpie":
                self._data_channel = channel
                self._setup_data_channel(channel)

        @pc.on("track")
        def on_track(track):
            if track.kind == "video":
                asyncio.ensure_future(self._receive_video(track))
            elif track.kind == "audio":
                asyncio.ensure_future(self._receive_audio(track))

    async def _create_offer(self):
        """Called when this peer wins role negotiation → becomes the offerer."""
        # Create the shared data channel
        dc_kwargs = {"ordered": self._options.data_channel_ordered}
        if self._options.data_channel_max_retransmits is not None:
            dc_kwargs["maxRetransmits"] = self._options.data_channel_max_retransmits

        dc = self._pc.createDataChannel("magpie", **dc_kwargs)
        self._data_channel = dc
        self._setup_data_channel(dc)

        # Add outbound media tracks
        self._pc.addTrack(self._video_track)
        self._pc.addTrack(self._audio_track)

        # Create and send SDP offer
        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)

        self._send_signal({
            "type":    "offer",
            "peer_id": self._peer_id,
            "sdp":     self._pc.localDescription.sdp,
        })
        Logger.debug(f"WebRTCConnection({self._peer_id}): SDP offer sent.")

    # ------------------------------------------------------------------
    # Internal: data channel
    # ------------------------------------------------------------------

    def _setup_data_channel(self, dc):
        @dc.on("open")
        def on_open():
            Logger.debug(f"WebRTCConnection({self._peer_id}): data channel open.")

        @dc.on("message")
        def on_message(data):
            try:
                msg = self._serializer.deserialize(data)
                self._route_data_message(msg)
            except Exception as e:
                Logger.warning(
                    f"WebRTCConnection({self._peer_id}): "
                    f"data channel message error: {e}"
                )

        @dc.on("close")
        def on_close():
            Logger.debug(f"WebRTCConnection({self._peer_id}): data channel closed.")

    def _route_data_message(self, msg: dict):
        """Dispatch an incoming data channel message to the right handler."""
        if not isinstance(msg, dict):
            return

        msg_type = msg.get("type")

        if msg_type == "pub":
            topic = msg.get("topic", "")
            payload = msg.get("payload")
            with self._routing_lock:
                callbacks = list(self._pub_callbacks.get(topic, []))
            for cb in callbacks:
                try:
                    cb(payload, topic)
                except Exception as e:
                    Logger.warning(
                        f"WebRTCConnection({self._peer_id}): "
                        f"pub callback error for topic '{topic}': {e}"
                    )

        elif msg_type == "rpc_req":
            service = msg.get("service", "")
            with self._routing_lock:
                q = self._rpc_service_queues.get(service)
            if q is not None:
                q.put_nowait(msg)
            else:
                Logger.warning(
                    f"WebRTCConnection({self._peer_id}): "
                    f"no handler registered for service '{service}'"
                )

        elif msg_type in ("rpc_ack", "rpc_rep"):
            rid = msg.get("rid")
            with self._routing_lock:
                cb = self._rpc_reply_callbacks.get(rid)
            if cb is not None:
                try:
                    cb(msg)
                except Exception as e:
                    Logger.warning(
                        f"WebRTCConnection({self._peer_id}): "
                        f"rpc reply callback error for rid='{rid}': {e}"
                    )

    # ------------------------------------------------------------------
    # Internal: media track reception
    # ------------------------------------------------------------------

    async def _receive_video(self, track):
        """Drain incoming video frames and dispatch to registered callbacks."""
        from luxai.magpie.frames.image import ImageFrameRaw
        Logger.debug(f"WebRTCConnection({self._peer_id}): receiving video track.")
        try:
            while not self._closing:
                av_frame = await track.recv()
                try:
                    arr = av_frame.to_ndarray(format="bgr24")
                    h, w, c = arr.shape
                    frame = ImageFrameRaw(
                        data=arr.tobytes(),
                        format="raw",
                        width=w,
                        height=h,
                        channels=c,
                        pixel_format="BGR",
                    )
                    with self._routing_lock:
                        callbacks = list(self._video_callbacks)
                    for cb in callbacks:
                        try:
                            cb(frame)
                        except Exception as e:
                            Logger.warning(
                                f"WebRTCConnection({self._peer_id}): "
                                f"video callback error: {e}"
                            )
                except Exception as e:
                    Logger.warning(
                        f"WebRTCConnection({self._peer_id}): "
                        f"video frame conversion error: {e}"
                    )
        except MediaStreamError:
            Logger.debug(f"WebRTCConnection({self._peer_id}): video track ended.")

    async def _receive_audio(self, track):
        """Drain incoming audio frames and dispatch to registered callbacks."""
        from luxai.magpie.frames.audio import AudioFrameRaw
        Logger.debug(f"WebRTCConnection({self._peer_id}): receiving audio track.")
        try:
            while not self._closing:
                av_frame = await track.recv()
                try:
                    import numpy as np
                    samples = av_frame.to_ndarray(format="s16")
                    # Planar (channels, samples) → interleaved
                    if samples.ndim == 2:
                        samples = samples.T.flatten()
                    frame = AudioFrameRaw(
                        data=samples.tobytes(),
                        channels=av_frame.channels,
                        sample_rate=av_frame.sample_rate,
                        bit_depth=16,
                        format="PCM",
                    )
                    with self._routing_lock:
                        callbacks = list(self._audio_callbacks)
                    for cb in callbacks:
                        try:
                            cb(frame)
                        except Exception as e:
                            Logger.warning(
                                f"WebRTCConnection({self._peer_id}): "
                                f"audio callback error: {e}"
                            )
                except Exception as e:
                    Logger.warning(
                        f"WebRTCConnection({self._peer_id}): "
                        f"audio frame conversion error: {e}"
                    )
        except MediaStreamError:
            Logger.debug(f"WebRTCConnection({self._peer_id}): audio track ended.")

    # ------------------------------------------------------------------
    # Internal: signaling
    # ------------------------------------------------------------------

    def _send_signal(self, msg: dict) -> None:
        """Serialize and publish a signaling message (callable from any thread)."""
        try:
            payload = self._serializer.serialize(msg)
            self._signaling.publish(self._signal_topic, payload)
        except Exception as e:
            Logger.warning(
                f"WebRTCConnection({self._peer_id}): signal send error: {e}"
            )

    def _on_signal_message(self, payload_bytes: bytes, topic: str) -> None:
        """
        Called by the signaling transport (e.g. MQTT callback thread) when a
        signaling message arrives.  Dispatches to the asyncio loop.
        """
        try:
            msg = self._serializer.deserialize(payload_bytes)
        except Exception as e:
            Logger.warning(
                f"WebRTCConnection({self._peer_id}): "
                f"failed to deserialize signal message: {e}"
            )
            return

        if not isinstance(msg, dict) or "type" not in msg:
            return

        # Ignore our own messages
        if msg.get("peer_id") == self._peer_id:
            return

        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._handle_signal(msg), self._loop)

    async def _handle_signal(self, msg: dict) -> None:
        """Process a signaling message inside the asyncio loop."""
        msg_type = msg.get("type")
        remote_peer_id = msg.get("peer_id", "")

        # ---- hello: remote peer announces presence ----
        if msg_type == "hello":
            if self._remote_peer_id is None:
                self._remote_peer_id = remote_peer_id
                Logger.debug(
                    f"WebRTCConnection({self._peer_id}): "
                    f"remote peer = {remote_peer_id}"
                )

            if not self._role_decided:
                self._role_decided = True
                if self._peer_id > remote_peer_id:
                    # We become the offerer
                    Logger.debug(
                        f"WebRTCConnection({self._peer_id}): role = offer"
                    )
                    asyncio.ensure_future(self._create_offer())
                else:
                    Logger.debug(
                        f"WebRTCConnection({self._peer_id}): role = answer"
                    )
                    # Reply immediately so the offerer can detect us even if
                    # it subscribed after we stopped broadcasting hellos.
                    self._send_signal({"type": "hello", "peer_id": self._peer_id})

        # ---- offer: remote peer sent SDP offer ----
        elif msg_type == "offer":
            if self._remote_peer_id is None:
                self._remote_peer_id = remote_peer_id

            sdp = msg.get("sdp", "")
            Logger.debug(f"WebRTCConnection({self._peer_id}): received SDP offer.")

            # Add outbound tracks before setting remote description
            self._pc.addTrack(self._video_track)
            self._pc.addTrack(self._audio_track)

            await self._pc.setRemoteDescription(
                RTCSessionDescription(sdp=sdp, type="offer")
            )
            await self._flush_pending_ice()

            answer = await self._pc.createAnswer()
            await self._pc.setLocalDescription(answer)

            self._send_signal({
                "type":    "answer",
                "peer_id": self._peer_id,
                "sdp":     self._pc.localDescription.sdp,
            })
            Logger.debug(f"WebRTCConnection({self._peer_id}): SDP answer sent.")

        # ---- answer: remote peer sent SDP answer ----
        elif msg_type == "answer":
            sdp = msg.get("sdp", "")
            Logger.debug(f"WebRTCConnection({self._peer_id}): received SDP answer.")
            await self._pc.setRemoteDescription(
                RTCSessionDescription(sdp=sdp, type="answer")
            )
            await self._flush_pending_ice()

        # ---- candidate: trickle ICE candidate ----
        elif msg_type == "candidate":
            candidate_str = msg.get("candidate", "")
            sdp_mid = msg.get("sdpMid")
            sdp_mline_index = msg.get("sdpMLineIndex")

            if not candidate_str:
                return

            try:
                from aiortc.sdp import candidate_from_sdp
                # Strip optional "candidate:" prefix
                raw = candidate_str
                if raw.startswith("candidate:"):
                    raw = raw[len("candidate:"):]
                candidate = candidate_from_sdp(raw)
                candidate.sdpMid = sdp_mid
                candidate.sdpMLineIndex = sdp_mline_index

                if self._pc.remoteDescription is None:
                    # Buffer until remote description is set
                    self._pending_ice_candidates.append(candidate)
                else:
                    await self._pc.addIceCandidate(candidate)
            except Exception as e:
                Logger.warning(
                    f"WebRTCConnection({self._peer_id}): "
                    f"ICE candidate error: {e}"
                )

    async def _flush_pending_ice(self):
        """Add buffered ICE candidates now that the remote description is set."""
        for candidate in self._pending_ice_candidates:
            try:
                await self._pc.addIceCandidate(candidate)
            except Exception as e:
                Logger.warning(
                    f"WebRTCConnection({self._peer_id}): "
                    f"buffered ICE candidate error: {e}"
                )
        self._pending_ice_candidates.clear()
