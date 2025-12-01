import time
import json
import socket
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, List

from luxai.magpie.utils import Logger


@dataclass
class NodeInfo:
    """Simple container for discovered node data."""
    node_id: str
    ips: List[str]      # all IPv4s from ServiceInfo
    port: int
    payload: Dict[str, Any]


class ZconfDiscovery:
    """
    Zeroconf/mDNS-based node discovery helper for Magpie.

    Key ideas:
        - Each node (e.g. robot, service) advertises itself as a Zeroconf service.
        - Other processes maintain an in-memory map of known nodes.
        - You can resolve a node_id to its current NodeInfo.

    Service model:
        - service_type: DNS-SD service type, e.g. "_magpie-zmq._tcp.local."
        - service_name: "<node_id>.<service_type>", e.g.
              "QTRD000320._magpie-zmq._tcp.local."
        - TXT properties:
              {
                  "node_id": "<node_id>",
                  "proto": "zmq",
                  "payload": "<JSON-encoded dict>"
              }

    Typical usage (advertiser side - e.g. on the robot):

        disc = ZconfDiscovery()
        disc.advertise_node(
            node_id="QTRD000320",
            port=50556,
            payload={"role": "robot", "model": "QTrobot"}
        )

    Typical usage (client side):

        disc = ZconfDiscovery()

        info = disc.resolve_node("QTRD000320", timeout=5.0)
        if info is None:
            raise RuntimeError("Node not found")
        ip = disc.pick_best_ip(info)
        endpoint = f"tcp://{ip}:{info.port}"
        robot = Robot.connect_zmq(endpoint=endpoint)

    The discovery is event-based (via ServiceBrowser). resolve_node() simply looks
    at the current view, optionally waiting up to `timeout` seconds for the node
    to appear.
    """

    def __init__(
        self,
        *,
        service_type: str = "_magpie-zmq._tcp.local."
    ) -> None:
        """
        Args:
            service_type:
                Zeroconf service type used to group all Magpie nodes that
                expose a ZMQ endpoint. All nodes using the same service_type
                will see each other.
                Example: "_magpie-zmq._tcp.local."
        """

        try:
            from zeroconf import IPVersion, ServiceBrowser, ServiceInfo, Zeroconf
        except ImportError:
            raise ImportError(
                "Zeroconf discovery requires the optional dependency 'zeroconf'. "
                "Install it via: pip install luxai-magpie[discovery]"
            )

        # Normalize service_type: must end with a dot, e.g. "_magpie-zmq._tcp.local."
        if not service_type.endswith("."):
            service_type += "."

        self._service_type = service_type

        # Underlying Zeroconf instance (handles interfaces, multicast, etc.)
        self._zc = Zeroconf(ip_version=IPVersion.V4Only)

        # Protects _nodes and condition waits
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)

        # node_id -> NodeInfo
        self._nodes: Dict[str, NodeInfo] = {}

        # Currently advertised service (if this process is advertising)
        self._advertised_info: Optional[ServiceInfo] = None

        # Listener that updates _nodes on add/update/remove
        class _Listener:
            def __init__(self, parent: "ZconfDiscovery") -> None:
                self._parent = parent

            def add_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
                # Logger.debug(f"[ZconfDiscovery] add_service: type={service_type}, name={name}")
                self._parent._on_service_added_or_updated(service_type, name)

            def update_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
                # Logger.debug(f"[ZconfDiscovery] update_service: type={service_type}, name={name}")
                self._parent._on_service_added_or_updated(service_type, name)

            def remove_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
                # Logger.debug(f"[ZconfDiscovery] remove_service: type={service_type}, name={name}")
                self._parent._on_service_removed(service_type, name)

        self._listener = _Listener(self)
        self._browser = ServiceBrowser(self._zc, self._service_type, self._listener)

    # ------------------------------------------------------------------
    # Advertising
    # ------------------------------------------------------------------
    def advertise_node(
        self,
        node_id: str,
        *,
        port: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Advertise this process as a Magpie node over Zeroconf.

        Args:
            node_id:
                Logical identifier of this node (e.g. "QTRD000320").
                Must be unique within the local network for this service_type.
            port:
                ZMQ (or other) TCP port clients should connect to.
            payload:
                Optional arbitrary metadata dict. It will be JSON-encoded into
                the TXT record under the "payload" key.
        """
        try:
            from zeroconf import ServiceInfo
        except ImportError:
            raise ImportError(
                "Zeroconf discovery requires the optional dependency 'zeroconf'. "
                "Install it via: pip install luxai-magpie[discovery]"
            )

        self.stop_advertising()

        # TXT properties: keys/values can be str; Zeroconf will encode them.
        txt_payload = json.dumps(payload or {})
        properties: Dict[str, Any] = {
            "node_id": node_id,
            "proto": "zmq",
            "payload": txt_payload,
        }

        # Service instance name: "<node_id>.<service_type>"
        service_name = f"{node_id}.{self._service_type}"

        # Advertise all usable IPv4 addresses for this host.
        ips = self._get_all_ipv4()
        if not ips:
            # Absolute fallback; in practice we should always find at least one non-loopback IP
            ips = ["127.0.0.1"]

        info = ServiceInfo(
            type_=self._service_type,
            name=service_name,
            addresses=[socket.inet_aton(ip) for ip in ips],
            port=port,
            properties=properties,
        )

        self._zc.register_service(info)
        self._advertised_info = info

    def stop_advertising(self) -> None:
        """Stop advertising this node if it is currently advertised."""
        if self._advertised_info is not None:
            try:
                self._zc.unregister_service(self._advertised_info)
            except Exception:
                # Ignore errors during unregister (e.g. if already closed)
                pass
            self._advertised_info = None

    # ------------------------------------------------------------------
    # Discovery / resolving
    # ------------------------------------------------------------------
    def resolve_node(self, node_id: str, timeout: float = 5.0) -> Optional[NodeInfo]:
        """
        Resolve a node_id to its current NodeInfo.

        This method:
            - returns immediately if the node is already known, or
            - waits up to `timeout` seconds for the node to appear via mDNS.

        Args:
            node_id:
                The logical ID of the node (e.g. "QTRD000320").
            timeout:
                Maximum time (in seconds) to wait for the node to show up.

        Returns:
            A NodeInfo instance, or None if not found within the timeout.
        """
        deadline = threading.current_thread()  # dummy to avoid linter warning; real code below
        deadline = None  # type: ignore[assignment]
        # Using time inside the lock to avoid subtle races
        import time as _time
        deadline = _time.time() + timeout

        with self._cond:
            while node_id not in self._nodes:
                remaining = deadline - _time.time()
                if remaining <= 0:
                    return None
                self._cond.wait(remaining)

            info = self._nodes[node_id]
            return info

    def pick_best_ip(self, node: NodeInfo) -> Optional[str]:
        """
        Pick the most suitable IP address from a NodeInfo.

        Heuristic:
            - Prefer non-loopback, non-link-local addresses.
            - If possible, prefer an address that shares a prefix with one
              of our own IPv4 addresses (same subnet heuristic).
            - Fallback to the first address if nothing better is found.
        """
        if not node.ips:
            return None

        # Get our own local IPv4 addresses for a simple "same subnet" heuristic.
        local_ips = self._get_all_ipv4()
        local_prefixes = {ip.rsplit(".", 1)[0] for ip in local_ips}

        def is_usable(ip: str) -> bool:
            return not (
                ip.startswith("127.") or
                ip.startswith("169.254.")
            )

        # 1) Prefer IPs on the same /24 as one of our local addresses.
        for ip in node.ips:
            if not is_usable(ip):
                continue
            prefix = ip.rsplit(".", 1)[0]
            if prefix in local_prefixes:
                return ip

        # 2) Then any usable IP.
        for ip in node.ips:
            if is_usable(ip):
                return ip

        # 3) Fallback: first IP, even if it's not ideal.
        return node.ips[0]

    def list_nodes(self) -> Dict[str, NodeInfo]:
        """
        Return a shallow copy of the currently known nodes.

        Returns:
            dict mapping node_id -> NodeInfo
        """
        with self._lock:
            return dict(self._nodes)

    # ------------------------------------------------------------------
    # Internal: listener hooks
    # ------------------------------------------------------------------
    def _on_service_added_or_updated(self, service_type: str, name: str) -> None:
        """
        Called by the listener when a service is added or updated.
        Resolves the service and updates self._nodes.
        """
        # Logger.debug(f"[ZconfDiscovery] _on_service_added_or_updated called: type={service_type}, name={name}")

        # Only care about our service_type
        if service_type != self._service_type:
            return

        # Resolve full ServiceInfo (may require network exchange)
        info = None
        tries = 0
        while info is None and tries < 4:
            tries += 1
            try:
                info = self._zc.get_service_info(service_type, name, timeout=2000)
            except Exception as e:
                # Logger.debug(f"[ZconfDiscovery] get_service_info raised on try {tries}: {e}")
                break

            if info is None:
                # Logger.debug(f"[ZconfDiscovery] get_service_info returned None on try {tries}, retrying...")
                # tiny sleep to let responses arrive
                time.sleep(0.2)

        if info is None:
            Logger.warning("[ZconfDiscovery] get_service_info returned None")
            return

        # Logger.debug(f"[ZconfDiscovery] Resolved service: port={info.port}, addresses={getattr(info, 'addresses', [])}")
        # Extract all IPv4 addresses if available
        ips = self._extract_ipv4(info)
        if not ips:
            return

        # Parse TXT properties (bytes -> str)
        props: Dict[str, Any] = {}
        for k, v in info.properties.items():
            key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
            if isinstance(v, (bytes, bytearray)):
                try:
                    props[key] = v.decode("utf-8")
                except UnicodeDecodeError:
                    props[key] = v.decode("latin-1", errors="replace")
            else:
                props[key] = str(v)

        node_id = props.get("node_id")
        if not node_id:
            # Fallback: derive node_id from service name "node_id.<service_type>"
            node_id = name.split(".", 1)[0]

        payload: Dict[str, Any]
        raw_payload = props.get("payload", "{}")
        try:
            payload = json.loads(raw_payload)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        node_info = NodeInfo(
            node_id=node_id,
            ips=ips,
            port=info.port,
            payload=payload,
        )

        with self._cond:
            self._nodes[node_id] = node_info
            self._cond.notify_all()

    def _on_service_removed(self, service_type: str, name: str) -> None:
        """
        Called by the listener when a service is removed.
        Removes the node from self._nodes if present.
        """
        if service_type != self._service_type:
            return

        # Best-effort derive node_id from service name
        node_id = name.split(".", 1)[0]

        with self._cond:
            if node_id in self._nodes:
                del self._nodes[node_id]
                self._cond.notify_all()

    @staticmethod
    def _extract_ipv4(info) -> list[str]:
        """
        Extract all IPv4 addresses from a ServiceInfo (if any).
        Zeroconf stores addresses as raw bytes (4 for IPv4, 16 for IPv6).
        """
        # Logger.debug(f"[ZconfDiscovery] _extract_ipv4: raw addresses={getattr(info, 'addresses', [])}")
        ips: list[str] = []
        for addr in getattr(info, "addresses", []):
            # IPv4 addresses are 4 bytes long
            if len(addr) == 4:
                try:
                    ip = socket.inet_ntoa(addr)
                    ips.append(ip)
                except OSError:
                    continue
        return ips

    @staticmethod
    def _get_all_ipv4() -> list[str]:
        """
        Best-effort: return all non-loopback IPv4 addresses for this host.

        This avoids relying on internet connectivity and lets us advertise
        all usable addresses (e.g. Wi-Fi + internal LAN) at once.
        """
        ips: set[str] = set()

        # Try resolving the hostname to IPv4 addresses.
        try:
            hostname = socket.gethostname()
            for family, _, _, _, sockaddr in socket.getaddrinfo(hostname, None):
                if family == socket.AF_INET:
                    ip = sockaddr[0]
                    ips.add(ip)
        except OSError:
            pass

        # Filter out obvious non-useful addresses (loopback, link-local).
        def _is_usable(ip: str) -> bool:
            return not (
                ip.startswith("127.") or      # loopback
                ip.startswith("169.254.")     # link-local
            )

        return [ip for ip in ips if _is_usable(ip)]

    # ------------------------------------------------------------------
    # Cleanup / context manager
    # ------------------------------------------------------------------
    def close(self) -> None:
        """
        Close the underlying Zeroconf instance and stop all discovery.

        After calling close(), this object should not be used anymore.
        """
        self.stop_advertising()
        try:
            self._zc.close()
        except Exception:
            pass

    def __enter__(self) -> "ZconfDiscovery":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
