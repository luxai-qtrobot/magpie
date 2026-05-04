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

MAGPIE is a **transport-agnostic messaging and RPC framework for developers and AI agents**.

Whether the wire is ZeroMQ, MQTT, WebRTC, or something entirely custom, the application layer never changes. Services built with MAGPIE are natively consumable by both code and AI tools via built-in MCP support — making it a natural integration engine for distributed systems, edge devices, and the next generation of AI-driven pipelines.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [ZMQ Streaming](#zmq-streaming)
  - [ZMQ Request / Response RPC](#zmq-request--response-rpc)
  - [MQTT Streaming](#mqtt-streaming)
  - [MQTT Request / Response RPC](#mqtt-request--response-rpc)
  - [MQTT Advanced Options](#mqtt-advanced-options)
  - [WebRTC Streaming](#webrtc-streaming)
  - [WebRTC Request / Response RPC](#webrtc-request--response-rpc)
  - [WebRTC Advanced Options](#webrtc-advanced-options)
  - [Schema-based RPC](#schema-based-rpc)
  - [MCP Integration](#mcp-integration)
  - [Network Discovery](#network-discovery)
- [CLI Tools](#cli-tools)
  - [ZMQ Tools](#zmq-tools)
  - [MQTT Tools](#mqtt-tools)
  - [WebRTC Tools](#webrtc-tools)
- [Architecture](#architecture)
- [Related Projects](#related-projects)
- [License](#license)

---

## Features

- **One API, any transport** — `StreamWriter`, `StreamReader`, `RpcRequester`, `RpcResponder` work identically over ZMQ, MQTT, and WebRTC; swap transports with one constructor change
- **Topic-based streaming** — high-throughput pub/sub via typed frames; publishers and subscribers are completely decoupled
- **Request / Response RPC** — synchronous request/reply with ACK, timeout, and per-call demux over any transport
- **Schema-based RPC** — JSON-RPC 2.0 dispatch via `JsonRpcSchema`; define your API once, call methods by name with the proxy interface (`client.add(a=3, b=4)`)
- **MCP support out of the box** — `McpSchema` turns any MAGPIE RPC responder into a fully compliant MCP tool server (`initialize`, `tools/list`, `tools/call`); `McpTransport` lets any FastMCP `Client` call those tools over ZMQ, MQTT, or WebRTC
- **MQTT transport** — full streaming and RPC over MQTT; shared connection; supports `mqtt://`, `mqtts://`, `ws://`, `wss://`, TLS, auth, LWT, and auto-reconnect
- **WebRTC transport** — P2P streaming, video/audio, and RPC over WebRTC; MQTT or ZMQ used only for the initial signaling handshake; STUN + optional TURN for NAT traversal
- **Typed frames** — `ImageFrameJpeg`, `ImageFrameCV`, `AudioFrameRaw`, `AudioFrameFlac`, and more; automatic serialization/deserialization across all transports
- **Node helpers** — `SourceNode`, `SinkNode`, `ProcessNode`, `ServerNode` add lifecycle and thread management on top of the raw transport primitives
- **Network discovery** — mDNS/Zeroconf node advertisement and scanning via `ZconfDiscovery`
- **CLI tools** — `magpie-write`, `magpie-read`, `magpie-request` and MQTT/WebRTC equivalents; video/audio capture and playback tools
- **Lightweight core** — ZeroMQ is the only core dependency; all media and protocol extras are opt-in

---

## Installation

### Core (ZMQ streaming + RPC)

```bash
pip install luxai-magpie
```

### Optional extras

| Extra | What it adds |
|---|---|
| `pip install "luxai-magpie[mqtt]"` | MQTT transport + MQTT CLI tools |
| `pip install "luxai-magpie[webrtc]"` | WebRTC transport — P2P streaming, video/audio, RPC over internet |
| `pip install "luxai-magpie[mcp]"` | MCP adapter — `McpTransport` for FastMCP `Client` |
| `pip install "luxai-magpie[audio]"` | Audio frames + capture/player CLI tools |
| `pip install "luxai-magpie[video]"` | Image frames + capture/viewer CLI tools |
| `pip install "luxai-magpie[discovery]"` | `magpie-discovery` CLI tool |
| `pip install "luxai-magpie[full]"` | All of the above |

---

## Quick Start

### ZMQ Streaming

**Writer:**

```python
import time
from luxai.magpie.transport import ZmqStreamWriter

writer = ZmqStreamWriter("tcp://*:5555")
i = 0
while True:
    try:
        writer.write({'id': i, 'value': 'hello'}, topic='/mytopic')
        i += 1
        time.sleep(1)
    except KeyboardInterrupt:
        writer.close()
        break
```

**Reader:**

```python
from luxai.magpie.transport import ZmqStreamReader

reader = ZmqStreamReader("tcp://127.0.0.1:5555", topic=['/mytopic'], bind=False)
while True:
    try:
        data, topic = reader.read()
        print(f"{topic}: {data}")
    except KeyboardInterrupt:
        reader.close()
        break
```

---

### ZMQ Request / Response RPC

**Responder:**

```python
from luxai.magpie.transport import ZMQRpcResponder

def handle(request):
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

---

### MQTT Streaming

MQTT uses a **shared connection** — create it once, pass it to any number of writers, readers, and RPC components.

**Writer:**

```python
from luxai.magpie.transport import MqttConnection, MqttStreamWriter

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

writer = MqttStreamWriter(conn)
writer.write({"sensor": "temp", "value": 22.5}, topic="sensors/temperature")

writer.close()
conn.disconnect()
```

**Reader:**

```python
from luxai.magpie.transport import MqttConnection, MqttStreamReader

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

reader = MqttStreamReader(conn, topic="sensors/temperature")
while True:
    try:
        data, topic = reader.read(timeout=5.0)
        print(f"{topic}: {data}")
    except KeyboardInterrupt:
        reader.close()
        break

conn.disconnect()
```

---

### MQTT Request / Response RPC

**Responder:**

```python
from luxai.magpie.transport import MqttConnection, MqttRpcResponder

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

def handle(request):
    return {"status": "ok", "echo": request}

server = MqttRpcResponder(conn, service_name="myservice/actions")
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

**Requester:**

```python
from luxai.magpie.transport import MqttConnection, MqttRpcRequester

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()

client = MqttRpcRequester(conn, service_name="myservice/actions")
try:
    response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
    print("Response:", response)
except TimeoutError:
    print("Request timed out")
finally:
    client.close()
    conn.disconnect()
```

---

### MQTT Advanced Options

```python
from luxai.magpie.transport import (
    MqttConnection, MqttOptions,
    MqttAuthOptions, MqttTlsOptions, MqttWillOptions, MqttDefaultsOptions,
)

conn = MqttConnection(
    "wss://broker.example.com:8884/mqtt",
    client_id="node-01",
    options=MqttOptions(
        auth=MqttAuthOptions(mode="username_password", username="node", password="secret"),
        tls=MqttTlsOptions(ca_file="/etc/ssl/certs/ca.pem", verify_peer=True),
        will=MqttWillOptions(enabled=True, topic="nodes/node-01/status",
                             payload="offline", qos=1, retain=True),
        defaults=MqttDefaultsOptions(publish_qos=1, subscribe_qos=1),
    ),
)
conn.connect()
```

---

### WebRTC Streaming

WebRTC enables **P2P communication over the internet** — no broker in the data path after the initial signaling handshake. Signaling is exchanged via MQTT (internet) or ZMQ (LAN).

Video and audio frames are carried over native WebRTC **RTP media tracks** when topics are declared in `WebRTCOptions`; all other data flows over the data channel.

**Writer (MQTT signaling):**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamWriter, WebRTCOptions

conn = WebRTCConnection.with_mqtt(
    "mqtt://broker.hivemq.com:1883", session_id="my-node",
    options=WebRTCOptions(video_topics=["/camera/color/image"]),
)
conn.connect()

writer = WebRtcStreamWriter(conn)
writer.write({"telemetry": [0.1, 0.2, 0.3]}, topic="service/state")   # → data channel
writer.write(ImageFrameRaw(...), topic="/camera/color/image")     # → RTP video track

writer.close()
conn.disconnect()
```

**Reader:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRtcStreamReader, WebRTCOptions

conn = WebRTCConnection.with_mqtt(
    "mqtt://broker.hivemq.com:1883", session_id="my-node",
    options=WebRTCOptions(video_topics=["/camera/color/image"]),
)
conn.connect()

reader  = WebRtcStreamReader(conn, topic="service/state")
vreader = WebRtcStreamReader(conn, topic="/camera/color/image")

data, _  = reader.read(timeout=5.0)
frame, _ = vreader.read(timeout=5.0)   # ImageFrameRaw

reader.close()
vreader.close()
conn.disconnect()
```

> **LAN / localhost:** replace `with_mqtt(...)` with `with_zmq("tcp://127.0.0.1:5555", ..., bind=True/False)` — no broker needed.

---

### WebRTC Request / Response RPC

No broker in the hot path — the data channel is bidirectional P2P, so no `reply_to` topic is needed.

**Responder:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcResponder

conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-node-rpc")
conn.connect()

def handle(request):
    return {"status": "ok", "echo": request}

server = WebRTCRpcResponder(conn, service_name="service/actions")
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

**Requester:**

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcRequester

conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="my-node-rpc")
conn.connect()

client = WebRTCRpcRequester(conn, service_name="service/actions")
try:
    response = client.call({"action": "move", "x": 1.0}, timeout=5.0)
    print("Response:", response)
except TimeoutError:
    print("Request timed out")
finally:
    client.close()
    conn.disconnect()
```

---

### WebRTC Advanced Options

```python
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCOptions, WebRTCTurnServer

opts = WebRTCOptions(
    stun_servers=["stun:stun.l.google.com:19302"],
    turn_servers=[WebRTCTurnServer(url="turn:myturn.server:3478", username="u", credential="p")],
    ice_transport_policy="all",          # "all" or "relay" (force TURN only)
    video_codec="H264",                  # "H264", "VP8", "VP9"
    video_bitrate=2000,                  # kbps
    video_topics=["/camera/color/image"],
    audio_topics=["/mic/audio/stream"],
    use_media_channels=True,
)
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", "my-node", options=opts)
conn.connect()
```

**Auto-reconnect:**

```python
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883",
                                   session_id="my-node", reconnect=True)
```

---

### Schema-based RPC

`JsonRpcSchema` adds JSON-RPC 2.0 dispatch on top of any MAGPIE transport. Define your API once — shape, description, and types — then attach handlers and call methods by name. The same schema object works on both sides.

**Responder — three ways to define methods:**

```python
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.schema import JsonRpcSchema

schema = JsonRpcSchema()

# Way A: decorator — shape and handler together (infers schema from type hints)
@schema.method()
def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b

# Way B: load from a standard JSON Schema file, attach handlers separately
schema2 = JsonRpcSchema.from_json_file("api.json")

@schema2.handler("convert")
def handle_convert(value, from_unit, to_unit):
    return {"result": value, "unit": to_unit}

# Way C: explicit register — full control over schema
schema.register(
    name="scale",
    func=lambda value, factor: value * factor,
    description="Scale a value",
    input_schema={
        "type": "object",
        "properties": {
            "value":  {"type": "number"},
            "factor": {"type": "number"},
        },
        "required": ["value", "factor"],
    },
)

server = ZMQRpcResponder("tcp://*:5556", schema=schema)
while True:
    try:
        server.handle_once(timeout=1.0)
    except TimeoutError:
        pass
    except KeyboardInterrupt:
        server.close()
        break
```

The JSON file (`api.json`) uses standard MCP/JSON Schema format. `description` and `outputSchema` are optional; `inputSchema` defaults to `{}` if omitted. `outputSchema` must describe an object (`"type": "object"`) — scalar return types are expressed only in `content[0].text`, not as structured output.

```json
[
  {
    "name": "convert",
    "description": "Convert a value from one unit to another",
    "inputSchema": {
      "type": "object",
      "properties": {
        "value":     {"type": "number"},
        "from_unit": {"type": "string"},
        "to_unit":   {"type": "string"}
      },
      "required": ["value", "from_unit", "to_unit"]
    }
  },
  {
    "name": "get_status",
    "description": "Return the current service status",
    "inputSchema": {
      "type": "object",
      "properties": {
        "service": {"type": "string"}
      },
      "required": ["service"]
    },
    "outputSchema": {
      "type": "object",
      "properties": {
        "status":  {"type": "string"},
        "uptime":  {"type": "number"}
      }
    }
  }
]
```

**Requester — proxy interface:**

```python
from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.schema import JsonRpcSchema, JsonRpcError

# Define shape on the requester side (stub bodies are fine)
schema = JsonRpcSchema()

@schema.method()
def add(a: float, b: float) -> float: ...

client = ZMQRpcRequester("tcp://127.0.0.1:5556", schema=schema)

# Proxy style — method name as attribute
result = client.add(a=3, b=4)               # → 7

# Base call style
result = client.call("add", a=3, b=4)       # → 7

# With explicit transport timeout
result = client.call("add", a=3, b=4, _timeout=5.0)

try:
    client.call("nonexistent")
except JsonRpcError as e:
    print(e.code, e.message)   # -32601 Method not found

client.close()
```

---

### MCP Integration

MAGPIE has native MCP support on both sides of the connection — no separate MCP server process required.

**Server side** — `McpSchema` extends `JsonRpcSchema` with the full MCP handshake. Any registered method is automatically exposed as an MCP tool.

**Agent / cloud side** — `McpTransport` is a FastMCP `ClientTransport` that wraps any MAGPIE `RpcRequester`. The caller creates and owns the requester; `McpTransport` borrows it.

The key value proposition: a service behind NAT connects **outbound** to a broker; an LLM agent on the cloud connects to the same broker. No port forwarding, no VPN.

```
pip install "luxai-magpie[mcp]"           # MCP + FastMCP (ZMQ always available)
pip install "luxai-magpie[mqtt,mcp]"      # add MQTT transport
pip install "luxai-magpie[webrtc,mcp]"    # add WebRTC transport
```

#### Server side — serve tools over any transport

```python
from luxai.magpie.schema import McpSchema

schema = McpSchema(name="my-service", version="1.0.0")

@schema.method()
def translate(text: str, target_lang: str) -> dict:
    """Translate text into the target language."""
    return {"translated": f"[{target_lang}] {text}", "lang": target_lang}

@schema.method()
def summarize(text: str, max_length: int) -> dict:
    """Summarize text to at most max_length characters."""
    return {"summary": text[:max_length]}
```

Attach to any responder:

```python
# ZMQ — no broker needed
from luxai.magpie.transport import ZMQRpcResponder
server = ZMQRpcResponder("tcp://*:5556", schema=schema)

# MQTT — service behind NAT
from luxai.magpie.transport.mqtt import MqttConnection
from luxai.magpie.transport import MqttRpcResponder
conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()
server = MqttRpcResponder(conn, service_name="node-01", schema=schema)

# WebRTC — P2P, lowest latency
from luxai.magpie.transport.webrtc import WebRTCConnection, WebRTCRpcResponder
conn = WebRTCConnection.with_mqtt("mqtt://broker.hivemq.com:1883", session_id="node-01")
conn.connect()
server = WebRTCRpcResponder(conn, service_name="node-01", schema=schema)
```

Serve loop is the same for all:

```python
while True:
    try:
        server.handle_once(timeout=1.0)
    except TimeoutError:
        pass
    except KeyboardInterrupt:
        server.close()
        break
```

#### Agent / cloud side — call tools with FastMCP Client

```python
import asyncio
from fastmcp import Client
from fastmcp.exceptions import ToolError
from luxai.magpie.adapters.mcp import McpTransport

# ZMQ
from luxai.magpie.transport import ZMQRpcRequester

async def main():
    req = ZMQRpcRequester("tcp://127.0.0.1:5556")

    async with Client(McpTransport(req)) as client:
        tools = await client.list_tools()
        for tool in tools:
            print(f"  {tool.name}: {tool.description}")

        result = await client.call_tool("translate", {"text": "Hello", "target_lang": "fr"})
        print(result.content[0].text)

        try:
            await client.call_tool("translate", {"text": "Hello"})   # missing target_lang
        except ToolError as e:
            print(f"tool error: {e}")

    req.close()

asyncio.run(main())
```

For MQTT or WebRTC, just swap the requester — `McpTransport` is identical:

```python
# MQTT
from luxai.magpie.transport.mqtt import MqttConnection
from luxai.magpie.transport import MqttRpcRequester

conn = MqttConnection("mqtt://broker.hivemq.com:1883")
conn.connect()
req = MqttRpcRequester(conn, service_name="node-01")

async with Client(McpTransport(req)) as client:
    result = await client.call_tool("translate", {"text": "Hello", "target_lang": "fr"})

req.close()
conn.disconnect()
```

#### Loading tools from an MCP tool-list file

```python
from luxai.magpie.schema import McpSchema

schema = McpSchema.from_json_file("tools.json")   # MCP native format

@schema.handler("translate")
def handle_translate(text: str, target_lang: str) -> dict:
    return {"translated": f"[{target_lang}] {text}", "lang": target_lang}
```

---

### Network Discovery

```python
from luxai.magpie.discovery import ZconfDiscovery

# Advertise a node
with ZconfDiscovery() as disc:
    disc.advertise_node("my-node", port=5555, payload={"role": "service"})
    input("Press Enter to stop advertising...")

# Discover nodes
with ZconfDiscovery() as disc:
    info = disc.resolve_node("my-node", timeout=5.0)
    if info:
        ip = disc.pick_best_ip(info)
        print(f"Found at tcp://{ip}:{info.port}")
```

---

## CLI Tools

### ZMQ Tools

```bash
pip install luxai-magpie
```

**`magpie-write`** — publish to a topic:
```bash
magpie-write tcp://127.0.0.1:5555 /mytopic '{"name": "Bob", "value": 42}'
magpie-write tcp://127.0.0.1:5555 /mytopic '{"x": 1}' --rate 10 --loop
magpie-write tcp://*:5555 /mytopic @payload.json --bind
```

**`magpie-read`** — subscribe and print:
```bash
magpie-read tcp://127.0.0.1:5555 /mytopic --pretty
magpie-read tcp://127.0.0.1:5555 /topic1 /topic2
```

**`magpie-request`** — send an RPC request:
```bash
magpie-request tcp://127.0.0.1:5556 '{"action": "greet", "name": "Bob"}' --pretty
magpie-request tcp://127.0.0.1:5556 '{"jsonrpc":"2.0","method":"add","params":{"a":3,"b":4},"id":1}' --pretty
```

**`magpie-discovery`** — mDNS node discovery:
```bash
magpie-discovery                                          # scan continuously
magpie-discovery --advertise --port 5555 --id MY_NODE     # advertise
```

**Audio / Video tools** (require `[audio]` / `[video]` extras):
```bash
magpie-video-capture tcp://*:5555 /camera --encoder jpeg
magpie-video-viewer  tcp://127.0.0.1:5555 /camera
magpie-audio-capture tcp://*:5556 /audio --samplerate 16000
magpie-audio-player  tcp://127.0.0.1:5556 /audio
```

---

### MQTT Tools

```bash
pip install "luxai-magpie[mqtt]"
```

**`magpie-write-mqtt`**:
```bash
magpie-write-mqtt mqtt://broker.hivemq.com:1883 /magpie/test '{"x": 1}' --rate 5 --loop
magpie-write-mqtt mqtt://broker.hivemq.com:1883 /magpie/status '{"state": "ready"}' --retain
```

**`magpie-read-mqtt`**:
```bash
magpie-read-mqtt mqtt://broker.hivemq.com:1883 /magpie/test --pretty
magpie-read-mqtt mqtt://broker.hivemq.com:1883 "/magpie/+" --hz    # wildcard + frequency
```

**`magpie-request-mqtt`**:
```bash
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myservice/actions '{"action": "run"}' --pretty
magpie-request-mqtt mqtt://broker.hivemq.com:1883 myservice/actions @req.json --timeout 10
```

> Advanced broker options (auth, TLS, QoS, LWT) can be passed via `--mqtt-params @params.json`.

---

### WebRTC Tools

```bash
pip install "luxai-magpie[webrtc,mqtt]"
```

Both peers use the same `session_id`. Signaling via `--signaling mqtt://...` (internet) or `--signaling tcp://...` (LAN, add `--bind` on one side).

**`magpie-write-webrtc` / `magpie-read-webrtc`**:
```bash
magpie-write-webrtc my-node /service/state '{"x": 1.0}' --signaling mqtt://broker.hivemq.com:1883
magpie-read-webrtc  my-node /service/state --signaling mqtt://broker.hivemq.com:1883 --pretty
```

**`magpie-request-webrtc`**:
```bash
magpie-request-webrtc my-node service/actions '{"action": "run"}' \
    --signaling mqtt://broker.hivemq.com:1883 --pretty
```

**Video / Audio over WebRTC**:
```bash
magpie-video-capture-webrtc my-node /camera --signaling mqtt://broker.hivemq.com:1883
magpie-video-viewer-webrtc  my-node /camera --signaling mqtt://broker.hivemq.com:1883
magpie-audio-capture-webrtc my-node /audio  --signaling mqtt://broker.hivemq.com:1883
magpie-audio-player-webrtc  my-node /audio  --signaling mqtt://broker.hivemq.com:1883
```

---

## Architecture

MAGPIE is built around four abstract base classes — `StreamWriter`, `StreamReader`, `RpcRequester`, `RpcResponder` — that absorb all threading, queuing, and lifecycle complexity. Transport implementations fill in two or three pure transport methods; everything else is handled by the base classes. This makes adding a new transport a matter of minutes, not days, and keeps user code completely transport-agnostic.

For the full architecture diagram, layer-by-layer breakdown, schema and MCP adapter design, and guides for adding new transports, serializers, and frame types, see [ARCHITECTURE.md](ARCHITECTURE.md).

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
