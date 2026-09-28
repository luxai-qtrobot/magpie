# Reusable HTTP signaling helper

Copy this directory into an existing Python web application. It uses only the
standard library and does not import MAGPIE. `SignalingHTTP` implements the
entire [wire contract](../../../docs/webrtc-http-signaling.md), while the ASGI
and WSGI classes expose it to common Python web hosts. Payloads are opaque.

```python
from http_signaling import InMemoryRelay, SignalingASGI, SignalingHTTP

relay_app = SignalingASGI(SignalingHTTP(InMemoryRelay()))
app.mount("/signal", relay_app)  # existing ASGI app
```

For a Flask app, mount the WSGI adapter with Werkzeug's dispatcher:

```python
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from http_signaling import InMemoryRelay, SignalingHTTP, SignalingWSGI

flask_app.wsgi_app = DispatcherMiddleware(
    flask_app.wsgi_app,
    {"/signal": SignalingWSGI(SignalingHTTP(InMemoryRelay()))},
)
```

Mounting either adapter supplies all four HTTP operations. The host is
responsible for authentication and session authorization. Flask route
decorators do not apply to a mounted WSGI app; wrap that app in auth middleware.

`InMemoryRelay` is for one process. A deployment with multiple workers or
serverless instances needs a shared mailbox store implementing `join`, `send`,
`receive`, and `leave`. The ASGI adapter uses a worker thread for each waiting
GET, so size the host accordingly. The runnable FastAPI example is the sibling
file `http_signaling_server.py`; FastAPI and Uvicorn are only required by that
example.
