import ssl
import sys
import threading
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id
from .mqtt_options import MqttOptions

try:
    import paho.mqtt.client as mqtt
    import importlib.metadata as _imeta
    _PAHO_MAJOR = int(_imeta.version("paho-mqtt").split(".")[0])
except ImportError:
    Logger.error(
        "Could not import paho-mqtt. "
        "Please install it using 'pip install paho-mqtt' or 'pip install luxai-magpie[mqtt]'."
    )
    sys.exit()


# Type alias for the per-message callback registered by subscribers / RPC components.
# Signature: callback(payload_bytes, topic_str)
MessageCallback = Callable[[bytes, str], None]


def _parse_mqtt_uri(uri: str) -> Tuple[str, int, str, bool, bool]:
    """
    Parse an MQTT URI into connection primitives.

    Supported schemes:
        mqtt://host:port        Plain TCP  (default port 1883)
        mqtts://host:port       TLS / TCP  (default port 8883)
        ws://host:port/path     WebSocket  (default port 9001)
        wss://host:port/path    TLS WS     (default port 8884)

    Returns:
        (host, port, ws_path, use_tls, use_websocket)
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme not in ("mqtt", "mqtts", "ws", "wss"):
        raise ValueError(
            f"Unsupported MQTT URI scheme '{scheme}'. "
            "Use mqtt://, mqtts://, ws://, or wss://."
        )

    host = parsed.hostname
    if not host:
        raise ValueError(f"Invalid MQTT URI: no host in '{uri}'")

    use_tls = scheme in ("mqtts", "wss")
    use_websocket = scheme in ("ws", "wss")

    default_ports = {"mqtt": 1883, "mqtts": 8883, "ws": 9001, "wss": 8884}
    port = parsed.port if parsed.port else default_ports[scheme]

    ws_path = parsed.path if parsed.path else "/mqtt"

    return host, port, ws_path, use_tls, use_websocket


class MqttConnection:
    """
    Shared MQTT broker connection.

    Create **one** instance per broker and pass it to ``MqttPublisher``,
    ``MqttSubscriber``, ``MqttRpcRequester``, and ``MqttRpcResponder``.
    This reuses a single TCP/WebSocket connection to the broker instead of
    opening a new one for every messaging component.

    The connection runs Paho's background network loop (``loop_start``), handles
    automatic reconnection, and re-subscribes all registered topics after
    every reconnect.

    Usage::

        conn = MqttConnection(
            "mqtts://broker.example.com:8883",
            client_id="robot-01",
            options=MqttOptions(
                auth=MqttAuthOptions(mode="username_password", username="u", password="p"),
            ),
        )
        conn.connect()          # blocks until connected or timeout

        pub = MqttPublisher(conn)
        sub = MqttSubscriber(conn, topic="sensors/temp")

        # ... use pub / sub ...

        pub.close()
        sub.close()
        conn.disconnect()
    """

    def __init__(
        self,
        uri: str,
        client_id: Optional[str] = None,
        protocol_version: int = 5,
        keepalive: int = 60,
        options: Optional[MqttOptions] = None,
    ):
        """
        Args:
            uri: MQTT broker URI. Supported schemes:

                - ``mqtt://host:port``   — plain TCP (default port 1883)
                - ``mqtts://host:port``  — TLS over TCP (default port 8883)
                - ``ws://host:port/path``  — WebSocket (default port 9001)
                - ``wss://host:port/path`` — TLS WebSocket (default port 8884)

            client_id: MQTT client identifier. Auto-generated (``magpie-<ulid>``) if omitted.
            protocol_version: MQTT protocol version — ``5`` (MQTTv5, default) or ``3``/``311`` (MQTTv3.1.1).
            keepalive: Keep-alive interval in seconds (default 60).
            options: Advanced options (TLS, auth, session, reconnect, will, QoS defaults).
                     Defaults to ``MqttOptions()`` (sensible defaults, no auth, no TLS).
        """
        self.uri = uri
        self.client_id = client_id or f"magpie-{get_uinque_id()[:12]}"
        self.keepalive = keepalive
        self.options = options or MqttOptions()

        # Parse URI
        self._host, self._port, self._ws_path, self._use_tls, self._use_websocket = (
            _parse_mqtt_uri(uri)
        )

        # Resolve Paho protocol constant
        if protocol_version == 5:
            self._mqtt_protocol = mqtt.MQTTv5
        elif protocol_version in (3, 311):
            self._mqtt_protocol = mqtt.MQTTv311
        else:
            raise ValueError(
                f"Unsupported MQTT protocol version {protocol_version}. Use 5 or 3."
            )

        # Create Paho client (handles API differences between paho v1 and v2)
        transport = "websockets" if self._use_websocket else "tcp"
        if _PAHO_MAJOR >= 2:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.client_id,
                protocol=self._mqtt_protocol,
                transport=transport,
            )
        else:
            self._client = mqtt.Client(
                client_id=self.client_id,
                protocol=self._mqtt_protocol,
                transport=transport,
            )

        # WebSocket path
        if self._use_websocket:
            self._client.ws_set_options(path=self._ws_path)

        # TLS — enabled when scheme is mqtts/wss, or when ca_file/cert_file is provided
        tls = self.options.tls
        if self._use_tls or tls.ca_file or tls.cert_file:
            self._configure_tls()

        # Authentication
        auth = self.options.auth
        if auth.mode in ("username_password", "both") and auth.username:
            self._client.username_pw_set(auth.username, auth.password)

        # Last Will and Testament
        will = self.options.will
        if will.enabled and will.topic:
            self._client.will_set(
                will.topic,
                payload=will.payload,
                qos=will.qos,
                retain=will.retain,
            )

        # Reconnect delay
        if self.options.reconnect.enabled:
            self._client.reconnect_delay_set(
                min_delay=max(1, int(self.options.reconnect.min_delay_sec)),
                max_delay=max(1, int(self.options.reconnect.max_delay_sec)),
            )

        # Subscription registry: topic_pattern -> list of callbacks
        # Protected by _sub_lock for thread-safe add/remove from any thread.
        self._subscriptions: Dict[str, List[MessageCallback]] = {}
        self._sub_lock = threading.Lock()

        # Connection state
        self._connected = False
        self._connect_event = threading.Event()
        self._closing = False

        # Attach Paho callbacks (version-aware)
        if _PAHO_MAJOR >= 2:
            self._client.on_connect = self._on_connect_v2
            self._client.on_disconnect = self._on_disconnect_v2
        else:
            self._client.on_connect = self._on_connect_v1
            self._client.on_disconnect = self._on_disconnect_v1
        self._client.on_message = self._on_message

        scheme_label = (
            "WSS" if (self._use_websocket and self._use_tls) else
            "WS" if self._use_websocket else
            "MQTTS" if self._use_tls else
            "MQTT"
        )
        Logger.debug(
            f"MqttConnection({self.client_id}): configured "
            f"{scheme_label} → {self._host}:{self._port}"
        )

    # ------------------------------------------------------------------
    # Internal: TLS setup
    # ------------------------------------------------------------------

    def _configure_tls(self):
        tls = self.options.tls
        cert_reqs = ssl.CERT_REQUIRED if tls.verify_peer else ssl.CERT_NONE

        # Map min_version string to ssl constant
        ver_str = tls.min_version.lower().replace(".", "").replace("v", "").replace("tls", "")
        if ver_str == "13":
            tls_version = ssl.PROTOCOL_TLS_CLIENT
        else:
            # tlsv1.2 and below — use TLS_CLIENT which negotiates the best available
            tls_version = ssl.PROTOCOL_TLS_CLIENT

        self._client.tls_set(
            ca_certs=tls.ca_file,
            certfile=tls.cert_file,
            keyfile=tls.key_file,
            cert_reqs=cert_reqs,
            tls_version=tls_version,
            keyfile_password=tls.key_password,
        )
        if not tls.verify_hostname:
            self._client.tls_insecure_set(True)

    # ------------------------------------------------------------------
    # Internal: Paho callbacks (v1 and v2 variants)
    # ------------------------------------------------------------------

    def _on_connect_v1(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True
            self._connect_event.set()
            Logger.debug(
                f"MqttConnection({self.client_id}): connected to {self._host}:{self._port}"
            )
            self._resubscribe_all()
        else:
            Logger.warning(
                f"MqttConnection({self.client_id}): connection refused (rc={rc})"
            )

    def _on_connect_v2(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            Logger.warning(
                f"MqttConnection({self.client_id}): connection failed: {reason_code}"
            )
        else:
            self._connected = True
            self._connect_event.set()
            Logger.debug(
                f"MqttConnection({self.client_id}): connected to {self._host}:{self._port}"
            )
            self._resubscribe_all()

    def _on_disconnect_v1(self, client, userdata, rc):
        self._connected = False
        if not self._closing:
            Logger.debug(
                f"MqttConnection({self.client_id}): disconnected (rc={rc}), reconnecting..."
            )

    def _on_disconnect_v2(self, client, userdata, disconnect_flags, reason_code, properties):
        self._connected = False
        if not self._closing:
            Logger.debug(
                f"MqttConnection({self.client_id}): disconnected ({reason_code}), reconnecting..."
            )

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload

        # Collect matching callbacks under lock, then dispatch outside lock to
        # avoid deadlocks if a callback tries to add/remove subscriptions.
        with self._sub_lock:
            matching = []
            for pattern, callbacks in self._subscriptions.items():
                if mqtt.topic_matches_sub(pattern, topic):
                    matching.extend(list(callbacks))

        for cb in matching:
            try:
                cb(payload, topic)
            except Exception as e:
                Logger.warning(
                    f"MqttConnection({self.client_id}): "
                    f"callback error for topic '{topic}': {e}"
                )

    def _resubscribe_all(self):
        """Re-subscribe to all registered topics after a reconnect."""
        with self._sub_lock:
            topics = list(self._subscriptions.keys())

        qos = self.options.defaults.subscribe_qos
        for topic in topics:
            try:
                self._client.subscribe(topic, qos=qos)
                Logger.debug(
                    f"MqttConnection({self.client_id}): re-subscribed to '{topic}'"
                )
            except Exception as e:
                Logger.warning(
                    f"MqttConnection({self.client_id}): "
                    f"re-subscribe error for '{topic}': {e}"
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def connect(self, timeout: float = 10.0) -> bool:
        """
        Connect to the MQTT broker and start the background network loop.

        Blocks until the connection is confirmed or *timeout* seconds elapse.

        Args:
            timeout: Maximum seconds to wait for the CONNACK (default 10).

        Returns:
            ``True`` if connected successfully, ``False`` on timeout.

        Raises:
            Exception: For network-level errors (e.g. host unreachable).
        """
        self._connect_event.clear()
        self._connected = False

        try:
            if self._mqtt_protocol == mqtt.MQTTv5:
                from paho.mqtt.properties import Properties
                from paho.mqtt.packettypes import PacketTypes
                props = Properties(PacketTypes.CONNECT)
                props.SessionExpiryInterval = self.options.session.session_expiry_sec
                self._client.connect(
                    self._host,
                    self._port,
                    keepalive=self.keepalive,
                    clean_start=self.options.session.clean_start,
                    properties=props,
                )
            else:
                self._client.connect(
                    self._host,
                    self._port,
                    keepalive=self.keepalive,
                )
        except Exception as e:
            Logger.error(f"MqttConnection({self.client_id}): connect error: {e}")
            raise

        self._client.loop_start()

        if not self._connect_event.wait(timeout=timeout):
            Logger.warning(
                f"MqttConnection({self.client_id}): "
                f"connect timed out after {timeout}s"
            )
            return False

        return True

    def disconnect(self):
        """
        Gracefully disconnect from the broker and stop the network loop.

        Does not close individual publisher/subscriber resources — call their
        ``close()`` methods before calling ``disconnect()``.
        """
        self._closing = True
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            Logger.warning(f"MqttConnection({self.client_id}): disconnect error: {e}")
        self._connected = False
        Logger.debug(f"MqttConnection({self.client_id}): disconnected.")

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
    ):
        """
        Publish a raw byte payload to the broker.

        Args:
            topic: MQTT topic string.
            payload: Raw bytes to publish.
            qos: QoS level (0, 1, or 2). Falls back to ``options.defaults.publish_qos``.
            retain: Retain flag. Falls back to ``options.defaults.publish_retain``.

        Raises:
            Exception: On transport-level publish failure.
        """
        if qos is None:
            qos = self.options.defaults.publish_qos
        if retain is None:
            retain = self.options.defaults.publish_retain

        try:
            result = self._client.publish(topic, payload=payload, qos=qos, retain=retain)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                Logger.warning(
                    f"MqttConnection({self.client_id}): "
                    f"publish rc={result.rc} on '{topic}'"
                )
        except Exception as e:
            Logger.warning(
                f"MqttConnection({self.client_id}): publish error on '{topic}': {e}"
            )
            raise

    def add_subscription(
        self,
        topic: str,
        callback: MessageCallback,
        qos: Optional[int] = None,
    ):
        """
        Register a *callback* for *topic* and subscribe on the broker (first registration only).

        Multiple callbacks may be registered for the same topic pattern; all will be
        invoked for each matching message.  MQTT wildcards (``+``, ``#``) are supported.

        Args:
            topic: MQTT topic or topic pattern.
            callback: ``callback(payload_bytes: bytes, topic: str) -> None``
            qos: QoS level. Falls back to ``options.defaults.subscribe_qos``.
        """
        if qos is None:
            qos = self.options.defaults.subscribe_qos

        with self._sub_lock:
            first_registration = topic not in self._subscriptions
            if first_registration:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(callback)

        # Subscribe on broker only once per unique topic pattern
        if first_registration and self._connected:
            try:
                self._client.subscribe(topic, qos=qos)
                Logger.debug(
                    f"MqttConnection({self.client_id}): "
                    f"subscribed to '{topic}' (qos={qos})"
                )
            except Exception as e:
                Logger.warning(
                    f"MqttConnection({self.client_id}): "
                    f"subscribe error for '{topic}': {e}"
                )

    def remove_subscription(self, topic: str, callback: MessageCallback):
        """
        Remove a *callback* for *topic*. Unsubscribes from broker when no callbacks remain.

        Args:
            topic: MQTT topic or pattern that was passed to ``add_subscription``.
            callback: The exact callback object to remove.
        """
        do_unsubscribe = False

        with self._sub_lock:
            if topic not in self._subscriptions:
                return
            try:
                self._subscriptions[topic].remove(callback)
            except ValueError:
                pass
            if not self._subscriptions[topic]:
                del self._subscriptions[topic]
                do_unsubscribe = True

        if do_unsubscribe and self._connected:
            try:
                self._client.unsubscribe(topic)
                Logger.debug(
                    f"MqttConnection({self.client_id}): unsubscribed from '{topic}'"
                )
            except Exception as e:
                Logger.warning(
                    f"MqttConnection({self.client_id}): "
                    f"unsubscribe error for '{topic}': {e}"
                )

    @property
    def is_connected(self) -> bool:
        """``True`` if currently connected to the broker."""
        return self._connected
