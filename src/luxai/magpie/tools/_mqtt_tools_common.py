"""
Shared helpers for MQTT CLI tools (mqtt_write, mqtt_read, mqtt_request).
"""
import argparse
import json
from typing import Optional

from luxai.magpie.transport import (
    MqttOptions,
    MqttTlsOptions,
    MqttAuthOptions,
    MqttSessionOptions,
    MqttReconnectOptions,
    MqttWillOptions,
    MqttDefaultsOptions,
)


def mqtt_params_type(raw: str) -> dict:
    """
    argparse type for ``--mqtt-params``.

    Accepts either:
      - A path prefixed with ``@`` pointing to a JSON file: ``@myparams.json``
      - An inline JSON object string: ``{"defaults": {"publish_qos": 2}}``

    Returns the parsed dict.
    """
    raw = raw.strip()

    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"mqtt-params file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(
                f"invalid JSON in mqtt-params file '{path}': {e}"
            )
    else:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(
                f"invalid JSON for --mqtt-params: {e}. "
                "Use an inline JSON object or @path/to/file.json"
            )

    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("--mqtt-params must be a JSON object")

    return data


def get_mqtt_protocol_version(d: Optional[dict]) -> int:
    """
    Extract the MQTT protocol version from a parsed ``--mqtt-params`` dict.

    Reads the optional top-level ``"protocol_version"`` key.  Valid values:

    * ``5``   — MQTTv5 (default)
    * ``3`` or ``311`` — MQTTv3.1.1 (required by brokers like Ably)
    """
    if not d:
        return 5
    version = d.get("protocol_version", 5)
    if version not in (3, 5, 311):
        raise ValueError(
            f"Invalid protocol_version {version!r}: use 3 (or 311) for MQTTv3.1.1, or 5 for MQTTv5"
        )
    return version


def build_mqtt_options(d: Optional[dict]) -> MqttOptions:
    """
    Convert a parsed ``--mqtt-params`` dict into an ``MqttOptions`` instance.

    Only keys present in *d* override the defaults; omitted sections keep
    their default values.

    Supported top-level keys (all optional):

    .. code-block:: json

        {
          "defaults": {
            "publish_qos":    1,
            "publish_retain": false,
            "subscribe_qos":  1
          },
          "auth": {
            "mode":     "username_password",
            "username": "robot",
            "password": "secret"
          },
          "tls": {
            "ca_file":          "/etc/ssl/certs/ca.pem",
            "cert_file":        "/etc/ssl/certs/client.crt",
            "key_file":         "/etc/ssl/private/client.key",
            "key_password":     null,
            "verify_peer":      true,
            "verify_hostname":  true,
            "min_version":      "tlsv1.2"
          },
          "session": {
            "clean_start":        true,
            "session_expiry_sec": 0
          },
          "reconnect": {
            "enabled":       true,
            "min_delay_sec": 1.0,
            "max_delay_sec": 30.0
          },
          "will": {
            "enabled": false,
            "topic":   "robot/status",
            "qos":     1,
            "retain":  true,
            "payload": "offline"
          }
        }
    """
    if not d:
        return MqttOptions()

    opts = MqttOptions()

    if "tls" in d:
        t = d["tls"]
        opts.tls = MqttTlsOptions(
            ca_file=t.get("ca_file"),
            cert_file=t.get("cert_file"),
            key_file=t.get("key_file"),
            key_password=t.get("key_password"),
            verify_peer=t.get("verify_peer", True),
            verify_hostname=t.get("verify_hostname", True),
            min_version=t.get("min_version", "tlsv1.2"),
        )

    if "auth" in d:
        a = d["auth"]
        opts.auth = MqttAuthOptions(
            mode=a.get("mode", "none"),
            username=a.get("username"),
            password=a.get("password"),
        )

    if "session" in d:
        s = d["session"]
        opts.session = MqttSessionOptions(
            clean_start=s.get("clean_start", True),
            session_expiry_sec=s.get("session_expiry_sec", 0),
        )

    if "reconnect" in d:
        r = d["reconnect"]
        opts.reconnect = MqttReconnectOptions(
            enabled=r.get("enabled", True),
            min_delay_sec=r.get("min_delay_sec", 1.0),
            max_delay_sec=r.get("max_delay_sec", 30.0),
        )

    if "will" in d:
        w = d["will"]
        opts.will = MqttWillOptions(
            enabled=w.get("enabled", False),
            topic=w.get("topic"),
            qos=w.get("qos", 1),
            retain=w.get("retain", True),
            payload=w.get("payload", "offline"),
        )

    if "defaults" in d:
        df = d["defaults"]
        opts.defaults = MqttDefaultsOptions(
            publish_qos=df.get("publish_qos", 1),
            publish_retain=df.get("publish_retain", False),
            subscribe_qos=df.get("subscribe_qos", 1),
        )

    return opts
