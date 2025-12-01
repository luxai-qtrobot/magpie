import json
import socket
import struct
import threading
import time
from typing import Any, Dict, List, Optional

from luxai.magpie.utils.logger import Logger

class McastDiscovery:
    """
    Simple UDP multicast-based discovery helper for magpie nodes.

    Wire format (JSON-encoded UTF-8):

        {
            "magic": "<magic string>",   # default: "magpie_discovery_v1"
            "ttl": <float>,              # seconds this beacon should be considered alive
            "payload": { ... }           # user-defined dictionary
        }

    Only `magic` and `ttl` are enforced. `payload` is completely user-defined.

    Typical usage:

        disc = McastDiscovery()

        # On the node you want to advertise:
        disc.advertise(
            payload={
                "node_id": "QTRD000320",
                ...
            },
            ttl=3.0,
            interval=1.0,
        )

        # On the node that wants to discover:
        beacons = disc.scan(timeout=2.5)
        for b in beacons:
            print(b["ttl"], b["payload"])

    Notes:
        - This class is intentionally generic and does NOT interpret the payload.
        - It uses IPv4 multicast (default 239.255.0.1:59000).
        - advertise() runs a background thread; stop_advertising() stops it.
    """

    def __init__(
        self,
        *,
        mcast_group: str = "239.20.20.20",
        mcast_port: int = 30000,
        magic: str = "magpie_discovery_v1",
        interface: str = "0.0.0.0",
    ) -> None:
        """
        Args:
            mcast_group: IPv4 multicast group address.
            mcast_port: UDP port used for discovery.
            magic: Magic string used to identify valid beacons.
            interface: Local interface IP for multicast (0.0.0.0 = default).
        """
        self._group = mcast_group
        self._port = mcast_port
        self._magic = magic
        self._interface = interface

        self._adv_thread: Optional[threading.Thread] = None
        self._adv_stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Advertising
    # ------------------------------------------------------------------
    def advertise(
        self,
        payload: Dict[str, Any],
        *,
        ttl: float = 3.0,
        interval: float = 1.0,
    ) -> None:
        """
        Start periodically broadcasting a discovery beacon in a background thread.

        The beacon has the shape:

            {
                "magic": self._magic,
                "ttl": ttl,
                "payload": payload
            }

        Args:
            payload: Arbitrary dict describing this node.
            ttl: How long (in seconds) receivers may consider this node alive
                 after the last seen beacon.
            interval: How often (in seconds) to send the beacon.

        Notes:
            - If advertise() is called while advertising is already running,
              the previous advertiser is stopped and replaced.
        """
        # Stop any existing advertiser
        self.stop_advertising()

        self._adv_stop_event = threading.Event()

        def _run() -> None:
            Logger.debug("starting mcast advertisement.")
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            try:
                # Set TTL for multicast packets so they stay in local network.
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

                # If a specific interface is requested, set it
                if self._interface not in ("", "0.0.0.0"):
                    sock.setsockopt(
                        socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(self._interface),
                    )

                addr = (self._group, self._port)

                while not self._adv_stop_event.is_set():
                    msg = {
                        "magic": self._magic,
                        "ttl": float(ttl),
                        "payload": payload,
                    }
                    data = json.dumps(msg).encode("utf-8")
                    try:
                        sock.sendto(data, addr)
                    except OSError:
                        # Ignore transient send errors in the advertiser
                        pass

                    # Wait for the next interval or stop request
                    if self._adv_stop_event.wait(interval):
                        break
            finally:
                sock.close()

        self._adv_thread = threading.Thread(target=_run, name="McastDiscoveryAdvertiser", daemon=True)
        self._adv_thread.start()

    def stop_advertising(self) -> None:        
        """Stop the background advertiser thread if it is running."""        
        if self._adv_thread is None:
            return
        Logger.debug("stopping mcast advertisement.")            
        self._adv_stop_event.set()
        self._adv_thread.join(timeout=1.0)
        self._adv_thread = None

    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------
    def scan(self, timeout: float = 2.0) -> List[Dict[str, Any]]:
        """
        Listen for beacons for up to `timeout` seconds and return all
        valid beacons received.

        Returns:
            A list of dicts with fields:
                {
                    "addr": {"ip": str, "port": int}
                    "ttl": float,
                    "payload": dict
                }

            Only packets with `magic == self._magic`, and having both
            `ttl` and `payload` fields, are returned.

        Notes:
            - This call is blocking.
            - It creates a fresh socket for each scan() to keep things simple.
        """
        results: List[Dict[str, Any]] = []

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            # Allow multiple listeners on the same address/port
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind to the multicast port on all interfaces
            sock.bind(("", self._port))

            # Join the multicast group
            group_bin = socket.inet_aton(self._group)
            if self._interface in ("", "0.0.0.0"):
                # Join on all interfaces (kernel decides which ones)
                mreq = struct.pack("4sl", group_bin, socket.INADDR_ANY)
            else:
                ifaddr = socket.inet_aton(self._interface)
                mreq = struct.pack("4s4s", group_bin, ifaddr)

            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            sock.settimeout(0.5)  # short per-recv timeout

            deadline = time.time() + timeout
            while time.time() < deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break

                # For the last iteration, clamp to remaining time
                per_recv_timeout = min(0.5, remaining)
                sock.settimeout(per_recv_timeout)

                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break

                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    # Ignore malformed packets
                    continue

                if not isinstance(msg, dict):
                    continue

                if msg.get("magic") != self._magic:
                    continue

                if "ttl" not in msg or "payload" not in msg:
                    continue

                try:
                    ttl_val = float(msg["ttl"])
                except (TypeError, ValueError):
                    continue

                payload = msg["payload"]
                if not isinstance(payload, dict):
                    continue

                sender_ip = addr[0] if isinstance(addr, tuple) and addr else None
                if not sender_ip:
                    continue

                sender_port = addr[1] if isinstance(addr, tuple) and addr else None
                
                entry = {
                    "addr": {"ip": sender_ip, "port": sender_port},
                    "ttl": ttl_val,
                    "payload": payload,
                }
                # Avoid duplicates:
                # treat (ip, payload) as identity; TTL doesn't matter for dedupe.
                if not any(
                    e["addr"]["ip"] == entry["addr"]["ip"] and e["payload"] == entry["payload"]
                    for e in results
                ):
                    results.append(entry)
                                    
        finally:
            try:
                # Attempt to drop membership; ignore errors
                group_bin = socket.inet_aton(self._group)
                ifaddr = socket.inet_aton(self._interface)
                mreq = struct.pack("4s4s", group_bin, ifaddr)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
            except OSError:
                pass
            sock.close()

        return results

    # ------------------------------------------------------------------
    # Context manager helpers (optional)
    # ------------------------------------------------------------------
    def __enter__(self) -> "McastDiscovery":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:        
        self.stop_advertising()
