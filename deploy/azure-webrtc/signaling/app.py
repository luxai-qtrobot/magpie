"""Public MAGPIE HTTP signaling relay with temporary Coturn credentials."""

from __future__ import annotations

import os
import re
import threading
import time
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from http_signaling import InMemoryRelay, SignalingASGI, SignalingHTTP
from turn_credentials import TurnCredentialIssuer


def _csv_env(name: str, default: str = "") -> list[str]:
    return [value.strip() for value in os.getenv(name, default).split(",") if value.strip()]


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}")
    return value


class FixedWindowRateLimiter:
    """Small in-process IP limiter suitable for a single container replica."""

    def __init__(self, requests_per_minute: int):
        self.limit = requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()
        self._last_cleanup = time.monotonic()

    def allow(self, key: str) -> tuple[bool, int]:
        now = time.monotonic()
        cutoff = now - 60.0
        with self._lock:
            if now - self._last_cleanup >= 60.0:
                for known_key, known_requests in list(self._requests.items()):
                    while known_requests and known_requests[0] <= cutoff:
                        known_requests.popleft()
                    if not known_requests:
                        self._requests.pop(known_key, None)
                self._last_cleanup = now
            requests = self._requests[key]
            while requests and requests[0] <= cutoff:
                requests.popleft()
            if len(requests) >= self.limit:
                retry_after = max(1, int(60.0 - (now - requests[0])))
                return False, retry_after
            requests.append(now)
            return True, 0


def _client_ip(request: Request) -> str:
    # Azure Container Apps supplies the client address in X-Forwarded-For.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[-1].strip()
    return request.client.host if request.client else "unknown"


max_sessions = _int_env("MAGPIE_MAX_SESSIONS", 16)
max_peers = _int_env("MAGPIE_MAX_PEERS_PER_SESSION", 4)
max_message_bytes = _int_env("MAGPIE_MAX_MESSAGE_BYTES", 256 * 1024)
max_queued_messages = _int_env("MAGPIE_MAX_QUEUED_MESSAGES", 256)
peer_lease_seconds = _int_env("MAGPIE_PEER_LEASE_SECONDS", 90)

relay = InMemoryRelay(
    lease_seconds=peer_lease_seconds,
    max_sessions=max_sessions,
    max_peers_per_session=max_peers,
    max_queued_messages=max_queued_messages,
)
signaling = SignalingHTTP(relay, max_message_bytes=max_message_bytes)

turn_urls = _csv_env("TURN_URLS")
stun_urls = _csv_env("STUN_URLS")
turn_secret = os.getenv("TURN_SHARED_SECRET", "")
turn_credential_ttl = _int_env("TURN_CREDENTIAL_TTL_SECONDS", 600, minimum=60)
credential_issuer = (
    TurnCredentialIssuer(turn_secret, ttl_seconds=turn_credential_ttl)
    if turn_secret and turn_urls
    else None
)

app = FastAPI(
    title="MAGPIE WebRTC signaling",
    description=(
        "An opaque HTTP signaling relay and short-lived Coturn credential issuer. "
        "It does not carry application data after WebRTC connects."
    ),
    version="1.0.0",
)

allowed_origins = _csv_env(
    "MAGPIE_SIGNAL_ALLOWED_ORIGINS",
    "https://magpie.luxai.com",
)
request_limiter = FixedWindowRateLimiter(
    _int_env("MAGPIE_REQUESTS_PER_MINUTE_PER_IP", 300)
)
ice_limiter = FixedWindowRateLimiter(
    _int_env("MAGPIE_ICE_REQUESTS_PER_MINUTE_PER_IP", 30)
)


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.url.path == "/healthz":
        return await call_next(request)

    client_ip = _client_ip(request)
    allowed, retry_after = request_limiter.allow(client_ip)
    if allowed and request.url.path.startswith("/ice/"):
        allowed, retry_after = ice_limiter.allow(client_ip)
    if not allowed:
        return JSONResponse(
            {"detail": "Rate limit exceeded"},
            status_code=429,
            headers={"Retry-After": str(retry_after), "Cache-Control": "no-store"},
        )
    return await call_next(request)


# Register CORS after the rate limiter so even 429 responses receive the
# appropriate browser headers.
local_origin_regex = os.getenv(
    "MAGPIE_SIGNAL_ALLOWED_ORIGIN_REGEX",
    r"https?://(?:localhost|127\.0\.0\.1)(?::\d+)?",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=local_origin_regex or None,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
    expose_headers=["X-Magpie-Sequence", "X-Magpie-Join-Announcements"],
)


@app.get("/", include_in_schema=False)
def service_info():
    return {
        "service": "MAGPIE WebRTC signaling",
        "signaling": "/signal",
        "temporaryIceConfiguration": "/ice/{session_id}",
        "health": "/healthz",
    }


@app.get("/healthz", include_in_schema=False)
def health():
    return {
        "status": "ok",
        "turnCredentialsConfigured": credential_issuer is not None,
    }


_SESSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~-]{2,127}$")


@app.get("/ice/{session_id}")
def ice_configuration(session_id: str):
    if not _SESSION_PATTERN.fullmatch(session_id):
        raise HTTPException(
            status_code=400,
            detail=(
                "session_id must contain 3-128 letters, numbers, dots, "
                "underscores, tildes, or hyphens"
            ),
        )
    if credential_issuer is None:
        raise HTTPException(
            status_code=503,
            detail="Temporary TURN credentials are not configured",
        )

    issued = credential_issuer.issue(session_id)
    return JSONResponse(
        {
            "expiresAt": issued.expires_at,
            "stunServers": stun_urls,
            "turnServers": [
                {
                    "url": url,
                    "username": issued.username,
                    "credential": issued.credential,
                }
                for url in turn_urls
            ],
        },
        headers={"Cache-Control": "no-store"},
    )


app.mount("/signal", SignalingASGI(signaling))
