"""Issue short-lived Coturn TURN REST API credentials."""

from __future__ import annotations

import base64
import hashlib
import hmac
import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class TurnCredential:
    username: str
    credential: str
    expires_at: int


class TurnCredentialIssuer:
    """Create and briefly cache one Coturn credential per MAGPIE session.

    Coturn's REST credential mechanism expects the username to start with a
    Unix expiration timestamp and the password to be a base64-encoded
    HMAC-SHA1 of that exact username. The MAGPIE session is represented by a
    short hash so peers in one session share Coturn's per-user quota without
    exposing the caller-chosen session ID in Coturn logs.
    """

    def __init__(
        self,
        shared_secret: str,
        *,
        ttl_seconds: int = 600,
        refresh_before_expiry_seconds: int = 60,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not shared_secret:
            raise ValueError("shared_secret must not be empty")
        if ttl_seconds < 60:
            raise ValueError("ttl_seconds must be at least 60")
        if not 0 <= refresh_before_expiry_seconds < ttl_seconds:
            raise ValueError("refresh_before_expiry_seconds must be below ttl_seconds")

        self._secret = shared_secret.encode("utf-8")
        self._ttl_seconds = ttl_seconds
        self._refresh_before_expiry_seconds = refresh_before_expiry_seconds
        self._clock = clock
        self._credentials: dict[str, TurnCredential] = {}
        self._lock = threading.Lock()

    def issue(self, session_id: str) -> TurnCredential:
        now = int(self._clock())
        session_hash = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:24]

        with self._lock:
            current = self._credentials.get(session_hash)
            if (
                current is not None
                and current.expires_at - now > self._refresh_before_expiry_seconds
            ):
                return current

            expires_at = now + self._ttl_seconds
            username = f"{expires_at}:{session_hash}"
            digest = hmac.new(self._secret, username.encode("utf-8"), hashlib.sha1).digest()
            credential = TurnCredential(
                username=username,
                credential=base64.b64encode(digest).decode("ascii"),
                expires_at=expires_at,
            )
            self._credentials[session_hash] = credential
            self._prune(now)
            return credential

    def _prune(self, now: int) -> None:
        expired = [
            session_hash
            for session_hash, credential in self._credentials.items()
            if credential.expires_at <= now
        ]
        for session_hash in expired:
            self._credentials.pop(session_hash, None)
