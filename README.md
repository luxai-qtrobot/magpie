<p align="center">
  <img src="https://github.com/luxai-qtrobot/magpie/raw/main/src/luxai/magpie/assets/magpie.png" alt="MAGPIE Logo" width="200"/>
</p>

<h1 align="center">MAGPIE</h1>
<p align="center"><em>Message Abstraction & General-Purpose Integration Engine</em></p>

<p align="center">
  <a href="https://github.com/luxai-qtrobot/magpie/actions/workflows/python-tests.yml">
    <img src="https://github.com/luxai-qtrobot/magpie/actions/workflows/python-tests.yml/badge.svg" alt="Test Status"/>
  </a>
  <a href="https://pypi.org/project/luxai-magpie/">
    <img src="https://img.shields.io/pypi/v/luxai-magpie" alt="PyPI version"/>
  </a>
  <a href="https://pypi.org/project/luxai-magpie/">
    <img src="https://img.shields.io/pypi/pyversions/luxai-magpie" alt="Python versions"/>
  </a>
  <a href="https://github.com/luxai-qtrobot/magpie/blob/main/LICENSE">
    <img src="https://img.shields.io/pypi/l/luxai-magpie" alt="License"/>
  </a>
</p>

---

MAGPIE is a lightweight, modular messaging engine for distributed Python systems. It provides a clean abstraction over pub/sub streams, request/response RPC, and network discovery — built on top of ZeroMQ, MQTT (via Paho), and WebRTC (via aiortc), with a pluggable transport layer.

Originally developed at **[LuxAI](https://luxai.com)** for the [QTrobot](https://luxai.com/qtrobot-for-research/) ecosystem, MAGPIE is generic enough for any Python-based distributed or AI pipeline.

---

## Features

- **Pub/Sub streaming** — high-throughput topic-based messaging via `StreamWriter` / `StreamReader`
- **Request/Response RPC** — synchronous and async-friendly RPC via `ZMQRpcRequester` / `ZMQRpcResponder`
- **MQTT transport** — full pub/sub and RPC over MQTT with a shared connection; supports `mqtt://`, `mqtts://`, `ws://`, `wss://`, TLS, auth, LWT, and auto-reconnect
- **WebRTC transport** — P2P pub/sub, video/audio streaming, and RPC over WebRTC; MQTT or ZMQ used for the initial signaling handshake, all payload traffic flows directly peer-to-peer; STUN + optional TURN for NAT traversal
- **Pluggable transports** — ZeroMQ, MQTT, and WebRTC today; add any custom transport without changing user code
- **Fast serialization** — msgpack by default; bring your own serializer via the abstract interface
- **Typed frames** — `ImageFrameJpeg`, `ImageFrameCV`, `AudioFrameRaw`, `AudioFrameFlac`, and more
- **Node helpers** — base classes (`SourceNode`, `SinkNode`, `ServerNode`, …) to build robust streaming services
- **Network discovery** — mDNS/Zeroconf node advertisement and scanning via `ZconfDiscovery`
- **CLI tools** — ready-to-use command-line tools for publishing, subscribing, RPC over both ZMQ and MQTT, video/audio streaming, and discovery
- **Lightweight core** — heavy media dependencies (NumPy, OpenCV, soundfile, aiortc) are fully opt-in

---

## Architecture

MAGPIE is built around four abstract base classes — `StreamWriter`, `StreamReader`, `RpcRequester`, `RpcResponder` — that absorb all threading, queuing, and lifecycle complexity. Transport implementations only fill in two or three pure transport methods; everything else is handled by the base classes. This makes adding a new transport a matter of minutes, not days, and means user code is completely transport-agnostic.

For the full architecture diagram, layer-by-layer breakdown, and guides for extending MAGPIE with new transports, custom serializers, and custom frame types, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Installation

### Core (pub/sub + RPC only)

```bash
pip install luxai-magpie
```

### Optional extras

| Extra | What it adds |
|---|---|
| `pip install "luxai-magpie[mqtt]"` | MQTT transport + MQTT CLI tools (paho-mqtt) |
| `pip install "luxai-magpie[audio]"` | Audio frames + capture/player CLI tools (numpy, soundfile, sounddevice) |
| `pip install "luxai-magpie[video]"` | Image frames + capture/viewer CLI tools (numpy, OpenCV, simplejpeg) |
| `pip install "luxai-magpie[discovery]"` | `magpie-discovery` CLI tool (zeroconf) |
| `pip install "luxai-magpie[webrtc]"` | WebRTC transport — P2P pub/sub, video/audio, RPC over internet (aiortc, numpy) |
| `pip install "luxai-magpie[full]"` | All of the above |

> **Note:** `magpie-publish`, `magpie-subscribe`, and `magpie-request` work with the base install — no extras needed (ZeroMQ is a core dependency). All CLI entry points are always registered; tools that require a missing extra will print a clear install instruction and exit.

---

## Supported Platforms

- **Python:** 3.8+
- **Linux** (x86\_64, ARM, Raspberry Pi, NVIDIA Jetson)
- **Windows**
- **macOS**

---

## Quick Start

### Pub/Sub

**Publisher:**

```python
import time
from luxai.magpie.transport import ZMQPublisher

publisher = ZMQPublisher("tcp://*:5555")
i = 0
while True:
    try:
        publisher.write({'id': i, 'value': 'hello'}, topic='/mytopic')
        i += 1
        time.sleep(1)
    except KeyboardInterrupt:
        publisher.close()
        break
```

**Subscriber:**

```python
from luxai.magpie.transport import ZMQSubscriber

subscriber = ZMQSubscriber("tcp://127.0.0.1:5555", topic=['/mytopic'], bind=False)
while True:
    try:
        data, topic = subscriber.read()
        print(f"{topic}: {data}")
    except KeyboardInterrupt:
        subscriber.close()
        break
```

### Request / Response RPC

**Requester:**

```python
from luxai.magpie.transport import ZMQRpcRequester

client = ZMQRpcRequester("tcp://127.0.0.1:5556")
try:
    response = client.call({'action': 'greet', 'name': 'Bob'}, timeout=3.0)
    print("Response:", response)
except TimeoutError:
    print("Request timed out")
finally:
    client.close()
```

**Responder:**

```python
from luxai.magpie.transport import ZMQRpcResponder

def handle(request):
    print("Got request:", request)
    return {'status': 'ok', 'echo': request}

server = ZMQRpcResponder("tcp://*:5556")
while True:
    try:
        server.handle_once(handler=handle, timeout=1.0)
    except TimeoutError:
        pass
    except KeyboardInterrupt:
        server.close()
        break
```

### MQTT Pub/Sub

MQTT transport uses a **shared connection** object — create it once and pass it to any number of publishers or subscribers.  All four URI schemes are supported out of the box.

**Publisher:**

```python
from luxai.magpie.transport import MqttConnection, MqttPublisher

conn = MqttConnection("mqtt://broker.hivemq.com:1883")   # or mqtts://, ws://, wss://
conn.connect()

pub = MqttPublisher(conn)
pub.write({"sensor": "temp", "value": 22.5}, topic="sensors/temperature")

pub.close()
conn.disconnect()
```

**Subscriber:**

```python
from luxai.magpie.transport import MqttConnection, MqttSubscriber

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

sub = MqttSubscriber(conn, topic="sensors/temperature")  # wildcards + and # supported
while True:
    try:
        data, topic = sub.read(timeout=5.0)
        print(f"{topic}: {data}")
    except KeyboardInterrupt:
        sub.close()
        break

conn.disconnect()
```

### MQTT Request / Response RPC

**Requester:**

```python
from luxai.magpie.transport import MqttConnection, MqttRpcRequester

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

client = MqttRpcRequester(conn, service_name="myrobot/motion")
try:
    response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
    print("Response:", response)
except TimeoutError:
    print("Request timed out")
finally:
    client.close()
    conn.disconnect()
```

**Responder:**

```python
from luxai.magpie.transport import MqttConnection, MqttRpcResponder

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

def handle(request):
    print("Got request:", request)
    return {"status": "ok", "echo": request}

server = MqttRpcResponder(conn, service_name="myrobot/motion")
while True:
    try:
        server.handle_once(handler=handle, timeout=1.0)
    except TimeoutError:
        pass
    except KeyboardInterrupt:
        server.close()
        break

conn.disconnect()
```

### MQTT Advanced Options

```python
from luxai.magpie.transport import (
    MqttConnection, MqttOptions,
    MqttAuthOptions, MqttTlsOptions, MqttWillOptions, MqttDefaultsOptions,
)

conn = MqttConnection(
    "wss://broker.example.com:8884/mqtt",
    client_id="robot-01",
    protocol_version=5,
    keepalive=60,
    options=MqttOptions(
        auth=MqttAuthOptions(
            mode="username_password",
            username="robot-01",
            password="secret",
        ),
        tls=MqttTlsOptions(
            ca_file="/etc/ssl/certs/ca.pem",
            verify_peer=True,
        ),
        will=MqttWillOptions(
            enabled=True,
            topic="robots/robot-01/status",
            payload="offline",
            qos=1,
            retain=True,
        ),
        defaults=MqttDefaultsOptions(publish_qos=1, subscribe_qos=1),
    ),
)
conn.connect()
```

### WebRTC Pub/Sub

WebRTC transport enables **P2P communication over the internet** — no broker in the data path after the initial handshake.  A `WebRTCConnection` is shared by all publishers and subscribers, mirroring the `MqttConnection` pattern.

Signaling (SDP offer/answer + ICE candidates) is exchanged via a **`WebRtcSignaler`** — an abstract transport that carries only the short handshake messages.  Two implementations are built in:

| Signaler | When to use |
|---|---|
| `MqttSignaler` | Internet / cross-network — requires an MQTT broker |
| `ZmqSignaler` | LAN / localhost — broker-less ZMQ PAIR socket |

Role negotiation (offer vs answer) is fully automatic.  `WebRTCPublisher` routes internally based on frame type and the `use_media_channels` option (default `True`):

| Frame type | `use_media_channels=True` | `use_media_channels=False` |
|---|---|---|
| `ImageFrame*` | Native **RTP video track** (H.264/VP8) — topic ignored | **Data channel**, JPEG-compressed, topic-routed |
| `AudioFrame*` | Native **RTP audio track** (Opus) — topic ignored | **Data channel**, topic-routed |
| Everything else | **Data channel**, topic-routed | **Data channel**, topic-routed |

With `use_media_channels=False`, video and audio frames are topic-routed just like regular data, enabling **multiple simultaneous video/audio topics** (e.g. two cameras on different topics).  Frames are auto-compressed to JPEG before sending (quality configurable via `media_channel_jpeg_quality`).

**Publisher (MQTT signaling — internet):**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher

# with_mqtt() creates the MqttSignaler and WebRTCConnection in one step.
# conn.disconnect() also disconnects the signaler — no separate teardown needed.
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-robot")
conn.connect()

pub = WebRTCPublisher(conn)
pub.write({"motor": [0.1, 0.2, 0.3]}, topic="robot/state")   # → data channel
pub.write(ImageFrameRaw(...))                                   # → video media track

pub.close()
conn.disconnect()
```

**Publisher (ZMQ signaling — LAN / localhost):**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCPublisher

# One peer binds (bind=True), the other connects (bind=False, the default).
conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", session_id="my-robot", bind=True)
conn.connect()

pub = WebRTCPublisher(conn)
pub.write({"motor": [0.1, 0.2, 0.3]}, topic="robot/state")

pub.close()
conn.disconnect()
```

**Subscriber:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCSubscriber

conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-robot")
# or: conn = WebRTCConnection.with_zmq("tcp://127.0.0.1:5555", session_id="my-robot", bind=False)
conn.connect()

sub  = WebRTCSubscriber(conn, topic="robot/state")                  # data channel topic
vsub = WebRTCSubscriber(conn, topic=WebRTCSubscriber.VIDEO_TOPIC)   # RTP video track (use_media_channels=True)
# vsub = WebRTCSubscriber(conn, topic="/camera")                    # data channel topic (use_media_channels=False)

data, _  = sub.read(timeout=5.0)
frame, _ = vsub.read(timeout=5.0)   # ImageFrameRaw (RTP) or ImageFrameJpeg (data channel)

sub.close()
vsub.close()
conn.disconnect()
```

### WebRTC Request / Response RPC

RPC over WebRTC uses the bidirectional data channel — no broker in the hot path, lower latency than MQTT RPC.  No `reply_to` topic is needed since the channel is P2P.

**Requester:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcRequester

conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-robot-rpc")
conn.connect()

client = WebRTCRpcRequester(conn, service_name="robot/motion")
try:
    response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
    print("Response:", response)
except TimeoutError:
    print("Request timed out")
finally:
    client.close()
    conn.disconnect()
```

**Responder:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcResponder

conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-robot-rpc")
conn.connect()

def handle(request):
    return {"status": "ok", "echo": request}

server = WebRTCRpcResponder(conn, service_name="robot/motion")
while True:
    try:
        server.handle_once(handler=handle, timeout=1.0)
    except TimeoutError:
        pass
    except KeyboardInterrupt:
        server.close()
        break

conn.disconnect()
```

### WebRTC Advanced Options

**Custom ICE/codec configuration:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCOptions, WebRTCTurnServer

opts = WebRTCOptions(
    stun_servers=["stun:stun.l.google.com:19302"],      # default
    turn_servers=[                                       # optional: strict NAT / corporate firewalls
        WebRTCTurnServer(
            url="turn:myturn.server:3478",
            username="user",
            credential="pass",
        )
    ],
    ice_transport_policy="all",                          # "all" or "relay" (force TURN only)
    data_channel_ordered=True,
    data_channel_max_retransmits=None,                   # None = reliable; 0 = fire-and-forget
    video_codec="H264",                                  # "H264", "VP8", "VP9"
    audio_codec="opus",
    video_bitrate=2000,                                  # kbps
    audio_bitrate=96,                                    # kbps
    use_media_channels=True,                             # False: route video/audio over data channel instead of RTP
    media_channel_jpeg_quality=80,                       # JPEG quality (1-100) when use_media_channels=False
)
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", "my-robot", options=opts)
```

**Automatic reconnection:**

```python
# reconnect=True: when the peer drops, the connection is re-established automatically.
# Frames sent during the reconnect gap are silently dropped.
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883",
                                   session_id="my-robot", reconnect=True)
```

**Using a signaler directly (advanced / custom transport):**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, MqttSignaler, ZmqSignaler

# MqttSignaler — wraps an MQTT connection internally
signaler = MqttSignaler("mqtt://broker.hivemq.com:1883", session_id="my-robot",
                        client_id="robot-side", timeout=10.0)
conn = WebRTCConnection(signaler=signaler, reconnect=True)
conn.connect()
# conn.disconnect() also calls signaler.disconnect()

# ZmqSignaler — broker-less PAIR socket (one side binds, the other connects)
signaler = ZmqSignaler("tcp://127.0.0.1:5555", session_id="my-robot", bind=True)
conn = WebRTCConnection(signaler=signaler)
conn.connect()
```

### Network Discovery

```python
from luxai.magpie.discovery import ZconfDiscovery

# Advertise a node
with ZconfDiscovery() as disc:
    disc.advertise_node("my-robot", port=5555, payload={"role": "robot"})
    input("Press Enter to stop advertising...")

# Discover nodes
with ZconfDiscovery() as disc:
    info = disc.resolve_node("my-robot", timeout=5.0)
    if info:
        ip = disc.pick_best_ip(info)
        print(f"Found at tcp://{ip}:{info.port}")
```

---

## ZMQ Command-Line Tools

`magpie-publish`, `magpie-subscribe`, and `magpie-request` work out of the box — no extras needed:

```bash
pip install luxai-magpie
```

Audio/video/discovery tools require their respective extra:

```bash
pip install "luxai-magpie[audio]"      # magpie-audio-capture, magpie-audio-player
pip install "luxai-magpie[video]"      # magpie-video-capture, magpie-video-viewer
pip install "luxai-magpie[discovery]"  # magpie-discovery
```

### `magpie-publish` — Publish a message to a topic

```bash
# Publish a dict payload once
magpie-publish tcp://127.0.0.1:5555 /mytopic '{"name": "Bob", "value": 42}'

# Publish at 10 Hz continuously
magpie-publish tcp://127.0.0.1:5555 /mytopic '{"x": 1}' --rate 10 --loop

# Publish a plain string (no DictFrame wrapping)
magpie-publish tcp://127.0.0.1:5555 /events "hello world" --raw

# Load payload from a JSON file
magpie-publish tcp://127.0.0.1:5555 /mytopic @payload.json --rate 5 --count 20

# Bind the socket (publisher listens, subscribers connect)
magpie-publish tcp://*:5555 /mytopic '{"status": "ok"}' --bind
```

### `magpie-subscribe` — Subscribe to a topic and print messages

```bash
# Subscribe to a single topic
magpie-subscribe tcp://127.0.0.1:5555 /mytopic

# Subscribe to multiple topics
magpie-subscribe tcp://127.0.0.1:5555 /topic1 /topic2

# Pretty-print JSON output
magpie-subscribe tcp://127.0.0.1:5555 /mytopic --pretty

# Bind the subscriber socket (publisher connects to it)
magpie-subscribe tcp://*:5555 /mytopic --bind
```

### `magpie-request` — Send an RPC request and print the response

```bash
# Send a request with a JSON payload
magpie-request tcp://127.0.0.1:5556 '{"action": "greet", "name": "Bob"}'

# Send from a JSON file with a 5 s timeout
magpie-request tcp://127.0.0.1:5556 @request.json --timeout 5.0

# Pretty-print the response
magpie-request tcp://127.0.0.1:5556 '{"query": "status"}' --pretty
```

### `magpie-video-capture` — Capture camera frames and stream over ZMQ

```bash
# Stream camera 0 in JPEG at 30 fps, binding on port 5555
magpie-video-capture tcp://*:5555 /camera --encoder jpeg

# Stream at 720p, 15 fps, connect to an existing subscriber
magpie-video-capture tcp://127.0.0.1:5555 /camera --size 1280 720 --framerate 15
```

### `magpie-video-viewer` — View a MAGPIE video stream

```bash
magpie-video-viewer tcp://127.0.0.1:5555 /camera
```

### `magpie-audio-capture` — Capture microphone audio and stream over ZMQ

```bash
# Stream at 16 kHz mono PCM (default)
magpie-audio-capture tcp://*:5556 /audio

# Stream at 48 kHz stereo, FLAC-compressed
magpie-audio-capture tcp://*:5556 /audio --samplerate 48000 --channels 2 --encoder flac

# Connect to a listening subscriber instead of binding
magpie-audio-capture tcp://127.0.0.1:5556 /audio
```

### `magpie-audio-player` — Receive and play a MAGPIE audio stream

```bash
magpie-audio-player tcp://127.0.0.1:5556 /audio
```

### `magpie-discovery` — Discover or advertise nodes on the local network

```bash
# Scan for nodes continuously (updates on change)
magpie-discovery

# Scan once and exit, with pretty output
magpie-discovery --once --pretty

# Advertise a node on port 5555
magpie-discovery --advertise --port 5555

# Advertise with a custom ID and metadata
magpie-discovery --advertise --port 5555 --id MY_ROBOT --payload '{"role": "robot", "model": "QTrobot"}'
```

---

## MQTT Command-Line Tools

Install with:

```bash
pip install "luxai-magpie[cli,mqtt]"
```

The MQTT tools mirror their ZMQ counterparts but target an MQTT broker instead of a ZMQ endpoint. The broker URI is a required positional argument. Advanced connection options (QoS, authentication, TLS, …) are loaded from a JSON file via `--mqtt-params @myparams.json`.

### `magpie-publish-mqtt` — Publish a message to an MQTT topic

```bash
# Publish a dict payload once
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/test "{'data': 'hello'}"

# Publish at 5 Hz continuously
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/test "{'x': 1}" --rate 5 --loop

# Publish a fixed number of messages
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/test "{'x': 1}" --rate 10 --count 20

# Publish a plain value without DictFrame wrapping
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/events "hello world" --raw

# Load payload from a JSON file
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/test @payload.json --rate 5

# Set the MQTT retain flag
magpie-publish-mqtt mqtt://broker.hivemq.com:1883 /magpie/status "{'state': 'ready'}" --retain

# Connect to a password-protected broker with custom QoS
magpie-publish-mqtt mqtt://broker.example.com:1883 /magpie/test "{'x': 1}" --mqtt-params @myparams.json
```

### `magpie-subscribe-mqtt` — Subscribe to an MQTT topic and print messages

```bash
# Subscribe to a topic
magpie-subscribe-mqtt mqtt://broker.hivemq.com:1883 /magpie/test

# Subscribe with MQTT wildcard patterns
magpie-subscribe-mqtt mqtt://broker.hivemq.com:1883 "/magpie/+"

# Pretty-print JSON output
magpie-subscribe-mqtt mqtt://broker.hivemq.com:1883 /magpie/test --pretty

# Receive one message and exit
magpie-subscribe-mqtt mqtt://broker.hivemq.com:1883 /magpie/test --once --pretty

# Show message frequency
magpie-subscribe-mqtt mqtt://broker.hivemq.com:1883 /magpie/test --hz

# Connect with authentication and TLS
magpie-subscribe-mqtt mqtts://broker.example.com:8883 /magpie/test --mqtt-params @myparams.json
```

### `magpie-request-mqtt` — Send an MQTT RPC request and print the response

```bash
# Send a request to a service
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myrobot/motion "{'action': 'move', 'x': 1.0}"

# Load request payload from a JSON file
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myrobot/motion @request.json

# Set call and ACK timeouts
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myrobot/motion "{'action': 'status'}" --timeout 10 --ack-timeout 3

# Pretty-print the response
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myrobot/motion "{'query': 'status'}" --pretty

# Connect with advanced params
magpie-request-mqtt mqtts://broker.example.com:8883 myrobot/motion "{'action': 'stop'}" --mqtt-params @myparams.json
```

### `--mqtt-params` JSON reference

Pass advanced broker connection options via `--mqtt-params @myparams.json`:

```json
{
  "defaults": {
    "publish_qos":    1,
    "subscribe_qos":  1,
    "publish_retain": false
  },
  "auth": {
    "mode":     "username_password",
    "username": "robot",
    "password": "secret"
  },
  "tls": {
    "ca_file":         "/etc/ssl/certs/ca.pem",
    "cert_file":       "/etc/ssl/certs/client.crt",
    "key_file":        "/etc/ssl/private/client.key",
    "verify_peer":     true,
    "verify_hostname": true
  },
  "reconnect": {
    "min_delay_sec": 1.0,
    "max_delay_sec": 30.0
  },
  "session": {
    "clean_start": true
  },
  "will": {
    "enabled": false,
    "topic":   "robot/status",
    "payload": "offline",
    "qos":     1,
    "retain":  true
  }
}
```

All sections are optional — omit any section to keep its default value.

---

## WebRTC Command-Line Tools

Install with:

```bash
pip install "luxai-magpie[webrtc,mqtt]"
```

WebRTC CLI tools always take a `session_id` positional argument — both peers must use the same value to find each other.  Signaling is configured via `--signaling URL`:

| URL scheme | Transport | Notes |
|---|---|---|
| `mqtt://host:port` | MQTT broker | Works over the internet; requires `[mqtt]` extra |
| `tcp://host:port` | ZMQ PAIR socket | Broker-less LAN; one side needs `--bind` |

### `magpie-publish-webrtc` — Publish messages over a WebRTC data channel

```bash
# Publish once via MQTT signaling (HiveMQ public broker)
magpie-publish-webrtc my-robot /robot/state '{"x": 1.0}' \
    --signaling mqtt://broker.hivemq.com:1883

# Publish at 10 Hz until stopped
magpie-publish-webrtc my-robot /robot/state '{"x": 1.0}' \
    --signaling mqtt://broker.hivemq.com:1883 --rate 10

# LAN / localhost: publisher binds the ZMQ signaling socket
magpie-publish-webrtc my-robot /robot/state '{"x": 1.0}' \
    --signaling tcp://127.0.0.1:5555 --bind
```

### `magpie-subscribe-webrtc` — Subscribe to a WebRTC data channel topic

```bash
# Subscribe via MQTT signaling
magpie-subscribe-webrtc my-robot /robot/state \
    --signaling mqtt://broker.hivemq.com:1883 --pretty

# LAN: subscriber connects (no --bind)
magpie-subscribe-webrtc my-robot /robot/state \
    --signaling tcp://127.0.0.1:5555

# Receive one message and exit
magpie-subscribe-webrtc my-robot /robot/state \
    --signaling mqtt://broker.hivemq.com:1883 --once

# Show message frequency
magpie-subscribe-webrtc my-robot /robot/state \
    --signaling mqtt://broker.hivemq.com:1883 --hz
```

### `magpie-request-webrtc` — Send an RPC request over a WebRTC data channel

```bash
# Send a request and print the response
magpie-request-webrtc my-robot robot/motion '{"action": "move", "x": 1.0}' \
    --signaling mqtt://broker.hivemq.com:1883 --pretty

# LAN
magpie-request-webrtc my-robot robot/motion '{"action": "move", "x": 1.0}' \
    --signaling tcp://127.0.0.1:5555
```

### `magpie-video-capture-webrtc` — Stream camera video over a WebRTC media track

```bash
# Stream camera 0 at 1280×720, 30 fps via MQTT signaling
magpie-video-capture-webrtc my-robot \
    --signaling mqtt://broker.hivemq.com:1883

# LAN: capture side binds the ZMQ signaling socket
magpie-video-capture-webrtc my-robot \
    --signaling tcp://127.0.0.1:5555 --bind

# Choose camera, resolution, and frame rate
magpie-video-capture-webrtc my-robot \
    --signaling mqtt://broker.hivemq.com:1883 \
    --camera 1 --size 640 480 --framerate 15
```

### `magpie-video-viewer-webrtc` — Receive and display a WebRTC video stream

```bash
# View via MQTT signaling
magpie-video-viewer-webrtc my-robot \
    --signaling mqtt://broker.hivemq.com:1883

# LAN: viewer connects (no --bind)
magpie-video-viewer-webrtc my-robot \
    --signaling tcp://127.0.0.1:5555
```

---

## Used in QTrobot

MAGPIE powers the internal messaging infrastructure of [QTrobot](https://luxai.com/qtrobot-for-research/) at **LuxAI**, handling audio/video streaming, distributed components, and SDK communication between robot subsystems.

---

## Project Status

**Status:** Beta — actively used in production-like systems. APIs are mostly stable; minor changes are still possible.

**Roadmap:**
- Multi-transport support (route the same stream over ZMQ and MQTT simultaneously)
- Higher-level pipeline abstractions for AI workloads

---

## Related Projects

| Project | Language | Repository |
|---|---|---|
| MAGPIE | Python | this repo |
| MAGPIE C++ | C++ (`libmagpie`, `libmagpie-mqtt`) | [luxai-qtrobot/magpie-cpp](https://github.com/luxai-qtrobot/magpie-cpp) |
| MAGPIE.js | TypeScript/JavaScript | [luxai-qtrobot/magpie-js](https://github.com/luxai-qtrobot/magpie-js) |

---

## License

Licensed under the [GNU General Public License v3 (GPLv3)](LICENSE).
