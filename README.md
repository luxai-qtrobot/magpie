<p align="center">
  <img src="src/magpie/assets/magpie.png" alt="MAGPIE Logo" width="200"/>
</p>

# MAGPIE – Message Abstraction & General-Purpose Integration Engine

> **MAGPIE is a lightweight, modular messaging engine providing high-performance pub/sub and RPC over pluggable transports.**


**Status:** Beta  
**PyPI package:** `luxai-magpie`  
**Repository:** `magpie` (GitHub, under LuxAI)

![Test Status](https://github.com/luxai-qtrobot/magpie/actions/workflows/python-tests.yml/badge.svg)


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
- 🧩 **Optional dependencies**
  - Core remains light; image/audio extras are opt-in

---

## Installation

Base installation (lightweight, no image/audio extras):

```bash
pip install luxai-magpie
```

### Optional extras

MAGPIE keeps heavy dependencies optional and uses **lazy imports** inside the library. Install only what you need:

```bash
# Audio-related frames (e.g. AudioFrameFlac)
pip install "luxai-magpie[audio]"

# Image-related frames (e.g. ImageFrameJpeg, ImageFrameCV)
pip install "luxai-magpie[video]"

# All media features
pip install "luxai-magpie[full]"

```

---

## Supported environment

- **Python:** 3.7.3 and newer
- **OS / platforms (tested)**
  - Linux (x86_64, ARM)
  - Windows
  - Raspberry Pi/NVIDIA Jetson (ARMv7 / ARM64)

---

## Quick Start Example

A minimal **pub/sub** example using the ZeroMQ transport.

### Publisher

```python
import time
from luxai.magpie.transport import ZMQPublisher
from luxai.magpie.utils import Logger

if __name__ == '__main__':    
    publisher = ZMQPublisher("tcp://*:5555")
    id = 1
    while True: 
        try:
            publisher.write({'name': 'Bob', 'last': 'Job'}, topic='/mytopic')
            Logger.info(f'publishing {id} ...')
            id = id + 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')
            publisher.close()
            break

```

### Subscriber

```python
import time
from luxai.magpie.transport import ZMQSubscriber
from luxai.magpie.utils import Logger

if __name__ == '__main__':
    Logger.set_level("DEBUG")
    subscriber = ZMQSubscriber("tcp://127.0.0.1:5555", topic=['/mytopic'], bind=False)

    while True: 
        try:
            data, topic = subscriber.read()            
            Logger.info(f"received {topic} : {data}")
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            subscriber.close()
            break    
```

Her is a minimal **req/resp** example using the ZeroMQ transport.

### Requester

```python
from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.utils import Logger

if __name__ == '__main__':
    Logger.set_level("DEBUG")
    client = ZMQRpcRequester("tcp://127.0.0.1:5556")
    try:            
        ret = client.call({'payload': 'hello'}, timeout=3.0)
        Logger.info(f"Got response {ret}")
    except TimeoutError:
        Logger.info('timeout')   
    
    client.close()
```

### Responder

```python
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.utils import Logger

def on_request(req : object):
    Logger.info(f"on_request: {req}")
    return req

if __name__ == '__main__':
    server = ZMQRpcResponder("tcp://*:5556")

    while True: 
        try:
            status = server.handle_once(handler=on_request, timeout=1.0)
        except TimeoutError:
            pass
        except KeyboardInterrupt:
            Logger.info('stopping...')
            server.close()
            break      
```

---

## Command-Line Tools (Optional)

MAGPIE provides lightweight CLI tools for streaming **video** and **audio** over ZeroMQ.

## Installation

```bash
pip install "luxai-magpie[cli]"
```

This installs the required optional dependencies and enables these CLI commands:

- `magpie-video-capture`
- `magpie-video-viewer`
- `magpie-audio-player`


## Video Streamer

Capture frames from an OpenCV camera and publish them over ZeroMQ.

```bash
magpie-video-capture tcp://*:5555 /camera --camera 0 --encoder jpeg
```

## Video Stream Viewer

Subscribe to a MAGPIE video stream and display it.

```bash
magpie-video-viewer tcp://127.0.0.1:5555 /camera
```

## Audio Stream Player

Receive and play audio frames in real time.

```bash
magpie-audio-player tcp://127.0.0.1:5555 /audio
```

--- 

## Architecture Overview

MAGPIE is organized into a few key modules:

### Transports (`magpie.transport.*`)

- **ZeroMQ-based** transport:
  - `zmq_publisher.py`, `zmq_subscriber.py`
  - `zmq_rpc_requester.py`, `zmq_rpc_responder.py`

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

- Bridging transport layers
- Implementing distributed components and SDKs for QTrobot
- Audio and video streaming between robot components

While MAGPIE is generic and not limited to robotics, its design is influenced by production use in embedded and robotics environments.

---

## Project status & roadmap

- **Status:** Beta
  - Actively used in production-like systems
  - APIs are mostly stable, but minor changes are still possible

-**Planned / potential enhancements:**
  - Additional transports (e.g. MQTT, WebRTC)
  - More serializers
  - Higher-level pipelines for AI workloads

---

## Contributing

Contributions are welcome! If you'd like to contribute:

1. Open an issue to discuss your idea or bug.
2. Keep changes focused and small where possible.
3. Add tests or simple examples when introducing new features.

---
## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
