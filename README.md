# MAGPIE – Message Abstraction & General-Purpose Integration Engine

> **MAGPIE is a lightweight, modular messaging engine providing high-performance pub/sub and RPC over pluggable transports.**

**Status:** Beta  
**License:** MIT  
**PyPI package:** `luxai-magpie`  
**Repository:** `magpie` (GitHub, under LuxAI)

MAGPIE is a small but powerful building block for distributed Python systems.  
It gives you a clean abstraction over:

- **Messaging patterns:** pub/sub streams and request/response RPC
- **Transports:** currently ZeroMQ, with a pluggable transport layer
- **Serialization:** abstract serializer interface, with msgpack implementation
- **Node helpers:** base classes for building streaming and RPC nodes
- **Frames:** typed data frames for audio, images, and generic payloads

Originally built for **QTrobot** at LuxAI, MAGPIE is generic enough to be used in any Python-based distributed system or AI pipeline.

---

## Features

- 📨 **High-level messaging API**
  - Stream-oriented **pub/sub** (`StreamWriter`, `StreamReader`)
  - **RPC** request/response (`RpcRequester`, `RpcResponder`)
- 🔌 **Pluggable transports**
  - ZeroMQ-based implementations (`magpie.transport.zmq.*`)
  - Local in-memory transport for testing
- 📦 **Serialization abstraction**
  - Serializer interface
  - Msgpack-based serializer by default
- 🧱 **Node helper classes**
  - Base node, process node, server node, source/sink node helpers
  - Facilities for threaded servers and callback-style processing
- 🧊 **Typed frames**
  - Generic `Frame` base class
  - Image frames: e.g. `ImageFrameJpeg`, `ImageFrameCV`
  - Audio frames: e.g. `AudioFrameRaw`, `AudioFrameFlac`
- 🧩 **Optional heavy dependencies**
  - Core remains light; image/audio extras are opt-in

---

## Installation

Base installation (lightweight, no image/audio extras):

```bash
pip install luxai-magpie
```

### Optional extras

MAGPIE keeps heavy dependencies optional and uses **lazy imports** inside the library.  
Install only what you need:

```bash
# Image-related frames (e.g. ImageFrameJpeg, ImageFrameCV)
pip install "luxai-magpie[image]"

# Audio-related frames (e.g. AudioFrameFlac)
pip install "luxai-magpie[audio]"
```

Typical extras (to be declared in `pyproject.toml`):

```toml
[project.optional-dependencies]
image = [
    "numpy",
    "simplejpeg",
    "opencv-python",
]

audio = [
    "numpy",
    "soundfile",
]
```

---

## Supported environment

- **Python:** 3.7.3 and newer
- **OS / platforms (tested)**
  - Linux (x86_64, ARM)
  - Windows
  - Raspberry Pi (ARMv7 / ARM64)
  - NVIDIA Jetson (JetPack 5.x)

---

## Quick Start Example

A minimal **pub/sub** example using the ZeroMQ transport.

### Publisher

```python
from magpie.transport.zmq.zmq_publisher import ZmqPublisher
from magpie.transport.zmq.zmq_utils import make_endpoint

if __name__ == "__main__":
    # Bind publisher to a TCP endpoint
    endpoint = make_endpoint(host="0.0.0.0", port=5555)
    pub = ZmqPublisher(endpoint)

    topic = "demo"
    message = {"msg": "hello from MAGPIE!"}

    pub.publish(message, topic=topic)
    print(f"Published on {endpoint} topic='{topic}': {message}")
```

### Subscriber

```python
from magpie.transport.zmq.zmq_subscriber import ZmqSubscriber
from magpie.transport.zmq.zmq_utils import make_endpoint

if __name__ == "__main__":
    # Connect to the same endpoint and subscribe to the topic
    endpoint = make_endpoint(host="127.0.0.1", port=5555)
    sub = ZmqSubscriber(endpoint, topics=["demo"])

    print("Waiting for message...")
    data, topic = sub.read()
    print(f"Received on topic='{topic}': {data}")
```

Run the subscriber first, then the publisher.  
Under the hood, this uses MAGPIE’s transport abstraction and serializer (msgpack by default).

> **RPC usage** is similarly simple using `RpcRequester` and `RpcResponder` in `magpie.transport.zmq`.  
> See the examples directory for a basic RPC echo server.

---

## Architecture Overview

MAGPIE is organized into a few key modules:

### Transports (`magpie.transport.*`)

- **ZeroMQ-based** transport:
  - `zmq_publisher.py`, `zmq_subscriber.py`
  - `zmq_rpc_requester.py`, `zmq_rpc_responder.py`
  - `zmq_utils.py`
- **Local in-memory** transport:
  - `transport.local.memory_pushpull` (useful for tests or single-process setups)

The transport layer is **pluggable**: you can add new transports (e.g. WebRTC, MQTT) without changing user-facing code.

### Serialization (`magpie.serializer.*`)

- `base_serializer.py` – abstract serializer interface
- `msgpack_serializer.py` – msgpack implementation

The default serializer is msgpack, but you can implement your own `BaseSerializer` if needed.

### Nodes (`magpie.nodes.*`)

Helper classes for building **long-running processes** and **streaming nodes**:

- `BaseNode` – common functionality (logging, main loop, etc.)
- `ProcessNode` – bidirectional process helpers
- `ServerNode` – RPC-style server helpers
- `SourceNode`, `SinkNode` – stream producers/consumers

These abstractions make it easier to build robust services that connect via MAGPIE streams and RPC.

### Frames (`magpie.frames.*`)

Typed data containers for structured payloads:

- `Frame` – base class
- `ImageFrameJpeg`, `ImageFrameCV` – image-specific frames
- `AudioFrameRaw`, `AudioFrameFlac` – audio-specific frames

Heavy dependencies (e.g. NumPy, OpenCV, soundfile) are **only imported when needed**, and can be installed via the optional extras.

---

## Used in QTrobot

MAGPIE is used internally at **LuxAI** as part of the QTrobot ecosystem, for example:

- Bridging transport layers (e.g. ZeroMQ ↔ ROS)
- Implementing distributed components and SDKs for QTrobot
- Audio and video streaming between robot components

While MAGPIE is generic and not limited to robotics, its design is influenced by production use in embedded and robotics environments.

---

## Project status & roadmap

- **Status:** Beta
  - Actively used in production-like systems
  - APIs are mostly stable, but minor changes are still possible

Planned / potential enhancements:

- Additional transports (e.g. MQTT, WebRTC)
- More serializers
- Higher-level pipelines for AI workloads

---

## Contributing

Contributions are welcome, but the project is primarily developed as part of LuxAI’s QTrobot stack.

If you’d like to contribute:

1. Open an issue to discuss your idea or bug.
2. Keep changes focused and small where possible.
3. Add tests or simple examples when introducing new features.

---

## License

MAGPIE is released under the **MIT License**.  
See `LICENSE` for details.
