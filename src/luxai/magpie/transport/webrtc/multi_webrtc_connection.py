"""One public WebRTC connection managing any number of remote peers.

Each remote peer owns its own RTCPeerConnection.  A single signaling transport
discovers peers and dispatches addressed negotiation messages to those links.
"""

import asyncio
import threading
import time
import uuid
from typing import Callable, Dict, List, Mapping, Optional

from luxai.magpie.serializer.msgpack_serializer import MsgpackSerializer
from luxai.magpie.utils.logger import Logger

from .http_signaler import HttpSignaler
from .webrtc_connection import _AIORTC_AVAILABLE, _PeerWebRTCConnection
from .webrtc_options import WebRTCOptions
from .webrtc_signaler import WebRtcSignaler


class _PeerSignaler(WebRtcSignaler):
    """A peer's view of the manager's shared signaling transport."""

    def __init__(self, owner: "WebRTCConnection", remote_peer_id: str):
        self._owner = owner
        self._remote_peer_id = remote_peer_id
        self._callback = None

    @property
    def session_id(self) -> str:
        return self._owner.session_id

    def publish(self, payload: bytes) -> None:
        message = self._owner._serializer.deserialize(payload)
        message["to_peer_id"] = self._remote_peer_id
        message["role"] = self._owner._role
        self._owner._signaler.publish(self._owner._serializer.serialize(message))

    def subscribe(self, callback: Callable[[bytes], None]) -> None:
        self._callback = callback

    def unsubscribe(self) -> None:
        self._callback = None

    def disconnect(self) -> None:
        self._callback = None

    def deliver(self, payload: bytes) -> None:
        callback = self._callback
        if callback is not None:
            callback(payload)


class _OriginQueue:
    """Remember the inbound peer without changing the RPC wire message."""

    def __init__(self, owner: "WebRTCConnection", remote_peer_id: str, queue):
        self._owner = owner
        self._remote_peer_id = remote_peer_id
        self._queue = queue

    def put_nowait(self, message):
        rid = message.get("rid")
        if rid:
            with self._owner._lock:
                self._owner._rpc_origins[rid] = self._remote_peer_id
        self._queue.put_nowait(message)


class WebRTCConnection:
    """Share publishers, readers and RPC services across WebRTC peers.

    ``connect()`` returns when the first peer connects.  Additional peers may
    join or leave while the connection remains active.  A WebRTC peer link is
    always one-to-one; this class owns one such link per remote peer.
    """

    def __init__(
        self,
        signaler: WebRtcSignaler,
        *,
        reconnect: bool = False,
        options: Optional[WebRTCOptions] = None,
        role: str = "mesh",
    ):
        if not _AIORTC_AVAILABLE:
            raise ImportError(
                "aiortc is required for WebRTC transport. "
                "Install with: pip install 'luxai-magpie[webrtc]'"
            )
        self._signaler = signaler
        self._reconnect = reconnect
        self._options = options or WebRTCOptions()
        if role not in ("mesh", "host", "client"):
            raise ValueError("role must be 'mesh', 'host', or 'client'")
        self._role = role
        self._use_media_channels = self._options.use_media_channels
        self._session_id = signaler.session_id
        self._peer_id = uuid.uuid4().hex[:16]
        self._serializer = MsgpackSerializer()
        self._lock = threading.RLock()
        self._peers: Dict[str, _PeerWebRTCConnection] = {}
        self._pub_callbacks = {}
        self._video_callbacks = {}
        self._audio_callbacks = {}
        self._rpc_services = {}
        self._rpc_reply_callbacks = {}
        self._rpc_origins = {}
        self._connected_event = threading.Event()
        self._stop = threading.Event()
        self._discovery_thread = None
        self._loop = None
        self._loop_thread = None
        self._started = False
        self._ever_connected = False
        self._restart_ids = {}

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def peer_id(self) -> str:
        return self._peer_id

    @property
    def peer_ids(self) -> List[str]:
        """IDs of currently connected remote peers."""
        with self._lock:
            return [pid for pid, peer in self._peers.items() if self._peer_ready(peer)]

    @property
    def is_connected(self) -> bool:
        return bool(self.peer_ids)

    def connect(self, timeout: Optional[float] = None) -> bool:
        """Start signaling and wait for the first connected remote peer."""
        with self._lock:
            if not self._started:
                if isinstance(self._signaler, HttpSignaler):
                    self._signaler.announce(
                        self._serializer.serialize(self._hello_message())
                    )
                self._started = True
                self._loop = asyncio.new_event_loop()
                self._loop_thread = threading.Thread(
                    target=self._run_loop, name="WebRTCLoop", daemon=True
                )
                self._loop_thread.start()
                self._signaler.subscribe(self._on_signal_message)
                self._discovery_thread = threading.Thread(
                    target=self._discovery_loop, name="WebRTCDiscovery", daemon=True
                )
                self._discovery_thread.start()
        try:
            deadline = time.monotonic() + timeout if timeout is not None else None
            while not self._stop.is_set():
                if self.is_connected:
                    return True
                remaining = deadline - time.monotonic() if deadline is not None else 0.5
                if remaining <= 0:
                    return False
                self._connected_event.wait(min(0.5, remaining))
                self._connected_event.clear()
            return False
        except KeyboardInterrupt:
            self.disconnect()
            raise

    def disconnect(self) -> None:
        self._stop.set()
        self._connected_event.set()
        self._signaler.unsubscribe()
        if self._discovery_thread and self._discovery_thread.is_alive():
            self._discovery_thread.join(timeout=2.0)
        with self._lock:
            peers = list(self._peers.values())
            self._peers.clear()
            self._rpc_origins.clear()
            self._restart_ids.clear()
        for peer in peers:
            try:
                peer.disconnect()
            except Exception as exc:
                Logger.warning(f"WebRTCConnection: peer cleanup failed: {exc}")
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=3.0)
        self._signaler.disconnect()
        self._connected_event.clear()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.close()

    @classmethod
    def with_mqtt(
        cls, broker_url: str, session_id: str, *, client_id=None,
        timeout: float = 10.0, mqtt_options=None, reconnect: bool = False,
        options: Optional[WebRTCOptions] = None, role: str = "mesh",
    ) -> "WebRTCConnection":
        from .webrtc_signaler import MqttSignaler
        signaler = MqttSignaler(
            broker_url, session_id, client_id=client_id,
            timeout=timeout, options=mqtt_options,
        )
        return cls(signaler, reconnect=reconnect, options=options, role=role)

    @classmethod
    def with_zmq(
        cls, endpoint: str, session_id: str, *, bind: bool = False,
        reconnect: bool = False, options: Optional[WebRTCOptions] = None,
        role: str = "mesh", multiplex: bool = False,
    ) -> "WebRTCConnection":
        from .webrtc_signaler import ZmqSignaler
        signaler = ZmqSignaler(endpoint, session_id, bind=bind, multiplex=multiplex)
        if options is None:
            options = WebRTCOptions(stun_servers=[])
        return cls(signaler, reconnect=reconnect, options=options, role=role)

    @classmethod
    def with_http(
        cls, base_url: str, session_id: str, *, participant_id=None,
        headers: Optional[Mapping[str, str]] = None,
        headers_provider: Optional[Callable[[], Mapping[str, str]]] = None,
        http_client=None, poll_wait: float = 20.0,
        request_timeout: float = 10.0, reconnect: bool = False,
        options: Optional[WebRTCOptions] = None, role: str = "mesh",
    ) -> "WebRTCConnection":
        from .http_signaler import HttpSignaler
        signaler = HttpSignaler(
            base_url, session_id, participant_id=participant_id,
            headers=headers, headers_provider=headers_provider,
            http_client=http_client, poll_wait=poll_wait,
            request_timeout=request_timeout,
            register_on_init=False,
        )
        try:
            return cls(signaler, reconnect=reconnect, options=options, role=role)
        except Exception:
            signaler.disconnect()
            raise

    def _hello_message(self) -> dict:
        return {
            "type": "hello", "peer_id": self._peer_id,
            "role": self._role,
            "audio_topics": self._options.audio_topics,
            "video_topics": self._options.video_topics,
        }

    def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            cached_join = (isinstance(self._signaler, HttpSignaler)
                           and self._signaler.supports_join_announcements)
            # A joining peer announces itself, so the established side does
            # not need a separate retry thread for every peer connection.
            if (not cached_join
                    and (not self._ever_connected or self._reconnect)):
                try:
                    self._signaler.publish(
                        self._serializer.serialize(self._hello_message())
                    )
                except Exception as exc:
                    Logger.warning(f"WebRTCConnection: discovery send failed: {exc}")
            with self._lock:
                stale = [
                    pid for pid, peer in self._peers.items()
                    if peer._connect_event.is_set() and not self._peer_ready(peer)
                ]
                peers = [self._peers.pop(pid) for pid in stale]
                for pid in stale:
                    self._drop_origins(pid)
            for peer in peers:
                try:
                    peer.disconnect()
                except Exception as exc:
                    Logger.warning(f"WebRTCConnection: stale peer cleanup failed: {exc}")
            if cached_join and self._reconnect:
                for remote in stale:
                    message = self._hello_message()
                    message["to_peer_id"] = remote
                    message["restart_id"] = uuid.uuid4().hex
                    try:
                        self._signaler.publish(self._serializer.serialize(message))
                    except Exception as exc:
                        Logger.warning(f"WebRTCConnection: recovery send failed: {exc}")
            self._stop.wait(1.0)

    def _drop_origins(self, peer_id: str) -> None:
        for rid, origin in list(self._rpc_origins.items()):
            if origin == peer_id:
                del self._rpc_origins[rid]

    def _on_peer_state(self, connected: bool) -> None:
        if connected:
            self._ever_connected = True
            self._connected_event.set()

    def _on_signal_message(self, payload: bytes) -> None:
        old_peer = None
        try:
            message = self._serializer.deserialize(payload)
            if not isinstance(message, dict):
                return
            remote = message.get("peer_id")
            if not isinstance(remote, str) or not remote or remote == self._peer_id:
                return
            remote_role = message.get("role", "mesh")
            if ((self._role == "client" and remote_role == "client")
                    or (self._role == "host" and remote_role == "host")):
                return
            target = message.get("to_peer_id")
            if target is not None and target != self._peer_id:
                return
            with self._lock:
                peer = self._peers.get(remote)
                restart_id = message.get("restart_id")
                new_restart = (
                    message.get("type") == "hello"
                    and isinstance(restart_id, str)
                    and restart_id != self._restart_ids.get(remote)
                )
                if new_restart:
                    self._restart_ids[remote] = restart_id
                if (peer is not None and message.get("type") == "hello"
                        and (new_restart or
                             (peer._connect_event.is_set() and not self._peer_ready(peer)))):
                    old_peer = self._peers.pop(remote)
                    self._drop_origins(remote)
                    peer = None
                if peer is None:
                    adapter = _PeerSignaler(self, remote)
                    peer = _PeerWebRTCConnection(
                        adapter, reconnect=False, options=self._options,
                        peer_id=self._peer_id, on_state_change=self._on_peer_state,
                        loop=self._loop,
                    )
                    self._peers[remote] = peer
                    self._register_on_peer(peer, remote)
                    peer.start()
                peer._signaler.deliver(payload)
        except Exception as exc:
            Logger.warning(f"WebRTCConnection: signaling dispatch failed: {exc}")
        finally:
            if old_peer is not None:
                try:
                    old_peer.disconnect()
                except Exception as exc:
                    Logger.warning(f"WebRTCConnection: old peer cleanup failed: {exc}")

    def _register_on_peer(self, peer: _PeerWebRTCConnection, remote: str) -> None:
        for topic, callbacks in self._pub_callbacks.items():
            for callback in callbacks:
                peer.add_pub_callback(topic, callback)
        for topic, callbacks in self._video_callbacks.items():
            for callback in callbacks:
                peer.add_video_callback(topic, callback)
        for topic, callbacks in self._audio_callbacks.items():
            for callback in callbacks:
                peer.add_audio_callback(topic, callback)
        for service, queue in self._rpc_services.items():
            peer.add_rpc_service(service, _OriginQueue(self, remote, queue))
        for rid, callback in self._rpc_reply_callbacks.items():
            peer.register_rpc_reply(rid, callback)

    def _iter_peers(self):
        with self._lock:
            return tuple(peer for peer in self._peers.values() if self._peer_ready(peer))

    @staticmethod
    def _peer_ready(peer: _PeerWebRTCConnection) -> bool:
        channel = peer._data_channel
        return bool(channel and channel.readyState == "open")

    def send_data(self, message: dict) -> None:
        kind = message.get("type") if isinstance(message, dict) else None
        if kind in ("rpc_ack", "rpc_rep"):
            rid = message.get("rid")
            with self._lock:
                remote = self._rpc_origins.get(rid)
                peer = self._peers.get(remote) if remote else None
                if kind == "rpc_rep":
                    self._rpc_origins.pop(rid, None)
            if peer is not None:
                peer.send_data(message)
            return
        for peer in self._iter_peers():
            peer.send_data(message)

    def send_media_frame(self, message: dict) -> None:
        for peer in self._iter_peers():
            peer.send_media_frame(message)

    def enqueue_media_send(self, message: dict) -> None:
        for peer in self._iter_peers():
            peer.enqueue_media_send(message)

    def add_pub_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            self._pub_callbacks.setdefault(topic, []).append(callback)
            for peer in self._peers.values():
                peer.add_pub_callback(topic, callback)

    def remove_pub_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            callbacks = self._pub_callbacks.get(topic, [])
            if callback in callbacks:
                callbacks.remove(callback)
            for peer in self._peers.values():
                peer.remove_pub_callback(topic, callback)

    def add_video_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            self._video_callbacks.setdefault(topic, []).append(callback)
            for peer in self._peers.values():
                peer.add_video_callback(topic, callback)

    def remove_video_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            callbacks = self._video_callbacks.get(topic, [])
            if callback in callbacks:
                callbacks.remove(callback)
            for peer in self._peers.values():
                peer.remove_video_callback(topic, callback)

    def add_audio_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            self._audio_callbacks.setdefault(topic, []).append(callback)
            for peer in self._peers.values():
                peer.add_audio_callback(topic, callback)

    def remove_audio_callback(self, topic: str, callback: Callable) -> None:
        with self._lock:
            callbacks = self._audio_callbacks.get(topic, [])
            if callback in callbacks:
                callbacks.remove(callback)
            for peer in self._peers.values():
                peer.remove_audio_callback(topic, callback)

    def add_rpc_service(self, service: str, queue) -> None:
        with self._lock:
            self._rpc_services[service] = queue
            for remote, peer in self._peers.items():
                peer.add_rpc_service(service, _OriginQueue(self, remote, queue))

    def remove_rpc_service(self, service: str) -> None:
        with self._lock:
            self._rpc_services.pop(service, None)
            for peer in self._peers.values():
                peer.remove_rpc_service(service)

    def register_rpc_reply(self, rid: str, callback: Callable) -> None:
        with self._lock:
            self._rpc_reply_callbacks[rid] = callback
            for peer in self._peers.values():
                peer.register_rpc_reply(rid, callback)

    def unregister_rpc_reply(self, rid: str) -> None:
        with self._lock:
            self._rpc_reply_callbacks.pop(rid, None)
            for peer in self._peers.values():
                peer.unregister_rpc_reply(rid)

    @property
    def audio_topics(self) -> List[str]:
        return list(self._options.audio_topics)

    @property
    def video_topics(self) -> List[str]:
        return list(self._options.video_topics)

    def is_audio_negotiated(self, topic: str) -> bool:
        return any(peer.is_audio_negotiated(topic) for peer in self._iter_peers())

    def is_video_negotiated(self, topic: str) -> bool:
        return any(peer.is_video_negotiated(topic) for peer in self._iter_peers())

    def get_audio_track(self, topic: str):
        return next((peer.get_audio_track(topic) for peer in self._iter_peers()), None)

    def get_video_track(self, topic: str):
        return next((peer.get_video_track(topic) for peer in self._iter_peers()), None)

