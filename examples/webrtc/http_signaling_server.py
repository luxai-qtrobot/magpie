"""FastAPI example using the reusable, MAGPIE-independent signaling relay.

Run from this repository: python examples/webrtc/http_signaling_server.py
Install this example's server dependencies: pip install fastapi uvicorn
The in-memory relay is suitable for one process only.
"""

import argparse
import os

from fastapi import FastAPI, Request
from fastapi.responses import Response

from http_signaling import InMemoryRelay, SignalingASGI, SignalingHTTP


app = FastAPI()
app.mount("/signal", SignalingASGI(SignalingHTTP(InMemoryRelay())))


@app.middleware("http")
async def optional_demo_token(request: Request, call_next):
    token = os.environ.get("MAGPIE_SIGNAL_TOKEN")
    if token and (request.url.path == "/signal" or request.url.path.startswith("/signal/")):
        if request.headers.get("Authorization") != "Bearer " + token:
            return Response(status_code=401)
    return await call_next(request)


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(description="FastAPI HTTP signaling example")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    print(f"Signaling relay: http://{args.host}:{args.port}/signal", flush=True)
    uvicorn.run(app, host=args.host, port=args.port)
