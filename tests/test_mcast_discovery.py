import json
import socket
import time
import unittest
from unittest import mock

from luxai.magpie.discovery import McastDiscovery


# ----------------------------------------------------------------------
# Fake sockets for advertise()
# ----------------------------------------------------------------------
class FakeAdvertiseSocket:
    def __init__(self, *args, **kwargs):
        self.setsockopt_calls = []
        self.sendto_calls = []
        self.closed = False

    def setsockopt(self, level, optname, value):
        self.setsockopt_calls.append((level, optname, value))

    def sendto(self, data, addr):
        self.sendto_calls.append((data, addr))

    def close(self):
        self.closed = True


# ----------------------------------------------------------------------
# Fake sockets for scan()
# ----------------------------------------------------------------------
class FakeScanSocket:
    def __init__(self, *args, **kwargs):
        self.setsockopt_calls = []
        self.bind_calls = []
        self.settimeout_calls = []
        self.recv_packets = []  # list of (data, addr)
        self.closed = False

    def setsockopt(self, level, optname, value):
        # scan() uses setsockopt for membership add/drop, ignore safely
        self.setsockopt_calls.append((level, optname, value))

    def bind(self, addr):
        self.bind_calls.append(addr)

    def settimeout(self, t):
        self.settimeout_calls.append(t)

    def recvfrom(self, bufsize):
        if self.recv_packets:
            return self.recv_packets.pop(0)
        # simulate timeout when no packets left
        raise socket.timeout()

    def close(self):
        self.closed = True


class TestMcastDiscovery(unittest.TestCase):
    # --------------------------------------------------------------
    # advertise()
    # --------------------------------------------------------------
    @mock.patch("socket.socket")
    def test_advertise_sends_beacons_and_stops(self, mock_socket_cls):
        fake_sock = FakeAdvertiseSocket()
        mock_socket_cls.return_value = fake_sock

        disc = McastDiscovery(mcast_group="239.1.2.3", mcast_port=40000)

        payload = {"node_id": "QTRD0001"}
        disc.advertise(payload=payload, ttl=1.5, interval=0.01)

        # Let the advertiser thread send a few packets
        time.sleep(0.05)
        disc.stop_advertising()

        # Should have sent at least one beacon
        self.assertGreaterEqual(len(fake_sock.sendto_calls), 1)

        data, addr = fake_sock.sendto_calls[0]
        self.assertEqual(addr, ("239.1.2.3", 40000))

        msg = json.loads(data.decode("utf-8"))
        self.assertEqual(msg["magic"], "magpie_discovery_v1")
        self.assertEqual(msg["payload"], payload)
        self.assertAlmostEqual(msg["ttl"], 1.5, places=2)

        self.assertTrue(fake_sock.closed)

    # --------------------------------------------------------------
    # scan(): filtering + dedup
    # --------------------------------------------------------------
    @mock.patch("socket.socket")
    def test_scan_filters_and_deduplicates(self, mock_socket_cls):
        fake_sock = FakeScanSocket()
        mock_socket_cls.return_value = fake_sock

        # Valid beacon
        valid1 = {
            "magic": "magpie_discovery_v1",
            "ttl": 3.0,
            "payload": {"node": "A"},
        }
        # Duplicate: same ip + payload, different ttl/port
        valid_dup = {
            "magic": "magpie_discovery_v1",
            "ttl": 5.0,
            "payload": {"node": "A"},
        }
        # Second distinct beacon (different ip or payload)
        valid2 = {
            "magic": "magpie_discovery_v1",
            "ttl": 4.0,
            "payload": {"node": "B"},
        }
        # Wrong magic
        wrong_magic = {
            "magic": "other_magic",
            "ttl": 1.0,
            "payload": {"node": "X"},
        }

        fake_sock.recv_packets = [
            (json.dumps(valid1).encode("utf-8"), ("10.0.0.1", 10001)),
            (json.dumps(valid_dup).encode("utf-8"), ("10.0.0.1", 10002)),  # duplicate
            (json.dumps(wrong_magic).encode("utf-8"), ("10.0.0.2", 10003)),  # ignored
            (b"not-json", ("10.0.0.3", 10004)),  # malformed, ignored
            (json.dumps(valid2).encode("utf-8"), ("10.0.0.4", 10005)),
        ]

        disc = McastDiscovery()
        results = disc.scan(timeout=0.1)

        # Expect 2 entries: valid1 + valid2 (duplicate collapsed)
        self.assertEqual(len(results), 2)

        # Sort by node id for stable assertions
        results_sorted = sorted(results, key=lambda e: e["payload"]["node"])

        first, second = results_sorted

        self.assertEqual(first["payload"], {"node": "A"})
        self.assertEqual(first["addr"]["ip"], "10.0.0.1")
        self.assertEqual(first["ttl"], 3.0)

        self.assertEqual(second["payload"], {"node": "B"})
        self.assertEqual(second["addr"]["ip"], "10.0.0.4")
        self.assertEqual(second["ttl"], 4.0)

        self.assertTrue(fake_sock.closed)

    # --------------------------------------------------------------
    # context manager: __enter__/__exit__
    # --------------------------------------------------------------
    @mock.patch.object(McastDiscovery, "stop_advertising")
    def test_context_manager_stops_advertising(self, mock_stop):
        with McastDiscovery() as disc:
            self.assertIsInstance(disc, McastDiscovery)

        # __exit__ should call stop_advertising
        mock_stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
