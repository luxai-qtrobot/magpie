from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MqttTlsOptions:
    """TLS/SSL configuration for the MQTT connection."""
    ca_file: Optional[str] = None           # Path to CA certificate file
    cert_file: Optional[str] = None         # Path to client certificate file (mTLS)
    key_file: Optional[str] = None          # Path to client private key file (mTLS)
    key_password: Optional[str] = None      # Password for the private key (if encrypted)
    verify_peer: bool = True                # Verify broker's certificate
    verify_hostname: bool = True            # Verify broker's hostname in certificate
    min_version: str = "tlsv1.2"           # Minimum TLS version (tlsv1.2 or tlsv1.3)


@dataclass
class MqttAuthOptions:
    """Authentication configuration for the MQTT connection."""
    mode: str = "none"                      # none | username_password | mtls | both
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class MqttSessionOptions:
    """MQTT session persistence configuration."""
    clean_start: bool = True                # Start a clean session (no persistence)
    session_expiry_sec: int = 0             # MQTTv5: session expiry interval in seconds


@dataclass
class MqttReconnectOptions:
    """Automatic reconnection configuration."""
    enabled: bool = True
    min_delay_sec: float = 1.0             # Minimum reconnect delay
    max_delay_sec: float = 30.0            # Maximum reconnect delay (exponential backoff)


@dataclass
class MqttWillOptions:
    """Last Will and Testament (LWT) configuration."""
    enabled: bool = False
    topic: Optional[str] = None
    qos: int = 1
    retain: bool = True
    payload: str = "offline"


@dataclass
class MqttDefaultsOptions:
    """Default QoS and retain settings for publish and subscribe."""
    publish_qos: int = 1
    publish_retain: bool = False
    subscribe_qos: int = 1


@dataclass
class MqttOptions:
    """
    Aggregated MQTT advanced options.

    Pass an instance of this to ``MqttConnection`` to configure TLS, authentication,
    session behaviour, reconnection, last will, and default QoS/retain values.

    Example::

        opts = MqttOptions(
            auth=MqttAuthOptions(mode="username_password", username="robot", password="secret"),
            tls=MqttTlsOptions(ca_file="/etc/ssl/certs/ca.pem"),
            defaults=MqttDefaultsOptions(publish_qos=1, subscribe_qos=1),
        )
        conn = MqttConnection("mqtts://broker.example.com:8883", options=opts)
    """
    tls: MqttTlsOptions = field(default_factory=MqttTlsOptions)
    auth: MqttAuthOptions = field(default_factory=MqttAuthOptions)
    session: MqttSessionOptions = field(default_factory=MqttSessionOptions)
    reconnect: MqttReconnectOptions = field(default_factory=MqttReconnectOptions)
    will: MqttWillOptions = field(default_factory=MqttWillOptions)
    defaults: MqttDefaultsOptions = field(default_factory=MqttDefaultsOptions)
