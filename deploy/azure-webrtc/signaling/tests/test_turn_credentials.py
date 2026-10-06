import base64
import hashlib
import hmac
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from turn_credentials import TurnCredentialIssuer


class TurnCredentialIssuerTests(unittest.TestCase):
    def test_credential_matches_coturn_rest_formula(self):
        now = [1_700_000_000]
        issuer = TurnCredentialIssuer(
            "test-shared-secret",
            ttl_seconds=600,
            clock=lambda: now[0],
        )

        issued = issuer.issue("alice-test-8f92c1")

        self.assertEqual(issued.expires_at, 1_700_000_600)
        self.assertTrue(issued.username.startswith("1700000600:"))
        expected = base64.b64encode(
            hmac.new(
                b"test-shared-secret",
                issued.username.encode("utf-8"),
                hashlib.sha1,
            ).digest()
        ).decode("ascii")
        self.assertEqual(issued.credential, expected)

    def test_session_reuses_credential_until_refresh_window(self):
        now = [1_700_000_000]
        issuer = TurnCredentialIssuer(
            "test-shared-secret",
            ttl_seconds=600,
            refresh_before_expiry_seconds=60,
            clock=lambda: now[0],
        )

        first = issuer.issue("one-session")
        now[0] += 500
        self.assertEqual(issuer.issue("one-session"), first)

        now[0] += 41
        refreshed = issuer.issue("one-session")
        self.assertNotEqual(refreshed.username, first.username)
        self.assertGreater(refreshed.expires_at, first.expires_at)

    def test_different_sessions_receive_different_turn_users(self):
        issuer = TurnCredentialIssuer(
            "test-shared-secret",
            ttl_seconds=600,
            clock=lambda: 1_700_000_000,
        )

        self.assertNotEqual(
            issuer.issue("session-one").username,
            issuer.issue("session-two").username,
        )


if __name__ == "__main__":
    unittest.main()
