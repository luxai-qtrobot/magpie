# MAGPIE Architecture

MAGPIE is built around a single guiding principle: **the user's code should never know or care which transport it is running on.**

A robot streaming video over ZeroMQ on a local network and the same robot streaming video to a remote operator over WebRTC look identical from the application layer. Swapping transports — or adding a new one — requires zero changes to user code.

---

## Architecture Diagram

<p align="center">
  <img src="https://github.com/luxai-qtrobot/magpie/raw/main/src/luxai/magpie/assets/magpie-architecture.jpg" alt="MAGPIE Architecture"/>
</p>

---

## Layer-by-Layer Breakdown

### User Application

The top of the stack. The user works with exactly four methods regardless of which transport is underneath:

| Pattern | Write side | Read side |
|---|---|---|
| Streaming | `writer.write(data, topic)` | `reader.read(timeout)` |
| RPC | `requester.call(request, timeout)` | `responder.handle_once(handler, timeout)` |

No transport-specific code ever leaks into user space.

---

### Transport Abstraction Layer

The heart of MAGPIE. Four abstract base classes define the **complete contract** for any transport implementation:

| ABC | Abstract methods | Handles for you |
|---|---|---|
| `StreamWriter` | `_transport_write()`, `_transport_close()` | Background write thread, queue management, drop-oldest policy, idempotent close |
| `StreamReader` | `_transport_read_blocking()`, `_transport_close()` | Background read thread, queue management, timeout, idempotent close |
| `RpcRequester` | `_transport_call()`, `_transport_close()` | Closed-check guard, error logging |
| `RpcResponder` | `_transport_recv()`, `_transport_send()`, `_transport_close()` | `handle_once()` orchestration — recv → call handler → send |

The base classes absorb all the complexity: threading, queuing, lifecycle, and error handling. A new transport only needs to fill in pure transport mechanics.

---

### Transport Implementations

Three transports are implemented today. Each follows the same pattern: a thin class that subclasses one of the four ABCs and implements only the transport-specific methods.

**ZMQ** — high-performance local/LAN messaging:
- `ZmqStreamWriter` / `ZmqStreamReader` — PUB/SUB sockets, topic-prefixed multipart frames
- `ZMQRpcRequester` / `ZMQRpcResponder` — DEALER/ROUTER sockets with a dedicated asyncio-style I/O thread and per-call ACK/reply demux

**MQTT** — broker-based messaging for LAN and internet:
- `MqttStreamWriter` / `MqttStreamReader` — publish/subscribe via a shared `MqttConnection`; MQTT push model bridged to pull via an internal `queue.Queue`
- `MqttRpcRequester` / `MqttRpcResponder` — RPC over MQTT topics with `rid`-based correlation and a `reply_to` topic per requester instance
- `MqttConnection` is the shared resource — one TCP connection to the broker multiplexed across all writers, readers, and RPC components

**WebRTC** — P2P streaming over the internet:
- `WebRtcStreamWriter` routes internally by frame type: `ImageFrame*` → native video media track (H.264/VP8), `AudioFrame*` → native audio media track (Opus), everything else → data channel (msgpack)
- `WebRtcStreamReader` / `WebRTCRpcRequester` / `WebRTCRpcResponder` — data channel and media track reception
- `WebRTCConnection` owns the `RTCPeerConnection`, runs an asyncio loop in a background thread, and handles signaling via any existing MAGPIE transport (MQTT or ZMQ) — role (offer/answer) is auto-negotiated, no user configuration required
- `WebRTCOptions` provides STUN/TURN server configuration, codec preferences, and session identification

---

### Wire Layer

The actual bytes on the network:

| Transport | Wire protocol |
|---|---|
| ZMQ | PUB/SUB + DEALER/ROUTER sockets over TCP, IPC, or inproc |
| MQTT | PUBLISH/SUBSCRIBE over TCP, TLS, or WebSocket via an MQTT broker |
| WebRTC | Data channel (SCTP/DTLS), video track (SRTP), audio track (SRTP) over UDP via ICE/STUN/TURN |

---

### Cross-Cutting Concerns

**Nodes** — optional high-level wrappers that add thread management, lifecycle control, and pause/resume support on top of the raw transport primitives. `SourceNode` wraps a `StreamWriter`, `SinkNode` wraps a `StreamReader`, `ProcessNode` combines both, and `ServerNode` wraps an `RpcResponder`. All four are transport-agnostic — the same node class works identically whether the underlying transport is ZMQ, MQTT, or WebRTC.

**Frames** — typed data containers that flow through every layer unchanged. All frames extend `Frame`, which provides automatic subclass registration for polymorphic serialization/deserialization. Adding a new frame type requires only a `@dataclass` with `Frame` as the base class.

**Serialization** — `BaseSerializer` is an ABC with two methods: `serialize(obj) → bytes` and `deserialize(bytes) → obj`. `MsgpackSerializer` is the default. Any serializer can be swapped in at construction time.

**Discovery** — `ZconfDiscovery` (mDNS/Zeroconf) and `McastDiscovery` (multicast) allow nodes to advertise and find each other on a local network without hardcoded IP addresses, complementing the ZMQ transport.

---

## Extending MAGPIE

### Adding a New Transport

Adding a transport — say, WebSocket or shared memory — requires implementing at most **five methods** split across two to four thin classes. The base classes handle everything else.

For streaming:

```python
from luxai.magpie.transport.stream_writer import StreamWriter
from luxai.magpie.transport.stream_reader import StreamReader

class MyStreamWriter(StreamWriter):
    def __init__(self, endpoint, queue_size=10):
        # set up your transport connection here
        super().__init__(name="MyStreamWriter", queue_size=queue_size)

    def _transport_write(self, data: object, topic: str):
        # serialize and send — that's it
        payload = self._serializer.serialize(data)
        self._socket.send(topic, payload)

    def _transport_close(self):
        self._socket.close()


class MyStreamReader(StreamReader):
    def __init__(self, endpoint, topic, queue_size=10):
        # set up your transport connection here
        super().__init__(name="MyStreamReader", queue_size=queue_size)

    def _transport_read_blocking(self, timeout=None):
        # block until data arrives, return (data, topic)
        raw, topic = self._socket.recv(timeout=timeout)
        return self._serializer.deserialize(raw), topic

    def _transport_close(self):
        self._socket.close()
```

For RPC:

```python
from luxai.magpie.transport.rpc_requester import RpcRequester
from luxai.magpie.transport.rpc_responder import RpcResponder

class MyRpcRequester(RpcRequester):
    def _transport_call(self, request_obj, timeout=None):
        # send request, wait for reply, return reply
        ...

    def _transport_close(self):
        ...


class MyRpcResponder(RpcResponder):
    def _transport_recv(self, timeout=None):
        # block until request arrives, return (request, client_ctx)
        ...

    def _transport_send(self, response_obj, client_ctx):
        # send response back to the client identified by client_ctx
        ...

    def _transport_close(self):
        ...
```

That is the complete implementation. The new transport is immediately usable everywhere MAGPIE components are used — nodes, tools, examples — without any other changes.

---

### Using a Custom Serializer

`BaseSerializer` defines two methods. Implement them to plug in any serialization format — JSON, protobuf, FlatBuffers, or a custom binary format:

```python
from luxai.magpie.serializer.base_serializer import BaseSerializer
import json

class JsonSerializer(BaseSerializer):
    def serialize(self, obj: object) -> bytes:
        return json.dumps(obj).encode()

    def deserialize(self, data: bytes) -> object:
        return json.loads(data.decode())
```

Pass it at construction time — the transport classes all accept a `serializer` parameter:

```python
pub = ZmqStreamWriter("tcp://*:5555", serializer=JsonSerializer())
sub = ZmqStreamReader("tcp://127.0.0.1:5555", serializer=JsonSerializer())

pub = MqttStreamWriter(conn, serializer=JsonSerializer())
sub = MqttStreamReader(conn, topic="sensors/temp", serializer=JsonSerializer())
```

---

### Adding a Custom Frame Type

All frames extend `Frame` and are registered automatically. Define a dataclass and the frame is immediately serializable and deserializable across the wire:

```python
from dataclasses import dataclass
from luxai.magpie.frames.frame import Frame

@dataclass
class LidarFrame(Frame):
    points: list = None      # list of (x, y, z) tuples
    timestamp: float = 0.0
    num_points: int = 0

    def __post_init__(self):
        super().__post_init__()
        if self.points:
            self.num_points = len(self.points)
```

No registration call needed. The `Frame` base class metaclass handles it. The frame can now be published and received through any MAGPIE transport:

```python
pub.write(LidarFrame(points=scan_points, timestamp=time.time()), topic="robot/lidar")

frame, topic = sub.read()   # frame is automatically reconstructed as LidarFrame
```
