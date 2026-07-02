# SSH over MQTT

`magpie-ssh-server-mqtt` and `magpie-ssh-mqtt` let you open a full SSH session
to **any machine that can reach an MQTT broker** — even if it is behind NAT, a
firewall, or has no public IP. No port forwarding, no VPN, no static IP needed.

## How it works

The server connects **outbound** to the broker and waits for session requests.
When a client connects, it sends an RPC request over MQTT; the server opens a
local TCP connection to `sshd`, and from that point the SSH byte stream flows
bidirectionally through the broker over per-session MQTT topics. The broker
never sees decrypted SSH traffic — the SSH encryption runs end-to-end inside
the MQTT messages.

```
ssh client
    │  stdin/stdout
    ▼
magpie-ssh-mqtt  ──── MQTT broker ────  magpie-ssh-server-mqtt
  (ProxyCommand)         (cloud)               (robot / server)
                                                      │
                                                  127.0.0.1:22
                                                    (sshd)
```

---

## Installation

```bash
pip install "luxai-magpie[mqtt]"
```

---

## Quick start

**1. Start the server on the remote machine:**

```bash
magpie-ssh-server-mqtt mqtt://mqtt.example.com:1883 my-robot
```

The server connects to the broker, subscribes to `magpie/ssh/my-robot/rpc/req`,
and waits. It forwards each accepted session to the local `sshd` on port 22.

**2. Connect from any client machine:**

```bash
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot
```

This opens an interactive SSH shell. Under the hood it generates a unique
session ID, sends an RPC request to the server, waits for confirmation that
`sshd` is reachable, then bridges `stdin`/`stdout` through the broker.

---

## Direct usage

### Interactive shell

```bash
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot
```

### Specify SSH user

```bash
# Short flag
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot -l alice

# user@host style (after node_id, passed through to ssh)
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot alice@my-robot
```

### Run a remote command

```bash
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot ls -la /home
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot uptime
```

### Custom SSH port or identity file

SSH flags go **after** `node_id` and are forwarded directly to `ssh`:

```bash
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot -p 2222
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot -i ~/.ssh/id_ed25519
```

> **Note:** Magpie flags (`--mqtt-params`, `--timeout`, `-v`) must come
> **before** `node_id`. SSH flags come after.

---

## ProxyCommand mode

ProxyCommand mode lets any tool that speaks standard SSH (`scp`, `rsync`,
VS Code Remote, `sftp`, `ssh-copy-id`, …) work transparently through the
MQTT tunnel — with no changes to those tools.

### One-off via `-o`

```bash
ssh -o "ProxyCommand=magpie-ssh-mqtt --proxy mqtt://mqtt.example.com:1883 %h" \
    alice@my-robot
```

### Persistent via `~/.ssh/config`

```sshconfig
Host my-robot
    ProxyCommand magpie-ssh-mqtt --proxy mqtt://mqtt.example.com:1883 %h
    User alice
```

Then all standard SSH tools work as if the machine were on your LAN:

```bash
ssh my-robot
scp my-robot:/var/log/syslog .
rsync -av my-robot:/data/ ./backup/
sftp my-robot
```

---

## VS Code Remote SSH

1. Install the **Remote - SSH** extension in VS Code.

2. Add the entry to your `~/.ssh/config`:

```sshconfig
Host my-robot
    ProxyCommand magpie-ssh-mqtt --proxy mqtt://mqtt.example.com:1883 %h
    User alice
```

3. Open the Command Palette → **Remote-SSH: Connect to Host…** → select
   `my-robot`.

VS Code connects through the MQTT tunnel exactly as it would over a direct SSH
connection. Port forwarding, terminal, file explorer, and extensions all work.

---

## Server options

| Flag | Default | Description |
|------|---------|-------------|
| `--sshd-host` | `127.0.0.1` | Host where the local `sshd` listens |
| `--sshd-port` | `22` | Port of the local `sshd` |
| `--timeout` | `10.0` | Broker connection timeout (seconds) |
| `--mqtt-params` | — | Auth / TLS / QoS options (see below) |
| `-v` | — | Enable DEBUG logging |

### Forward to a non-default sshd

```bash
# sshd listening on a different port
magpie-ssh-server-mqtt mqtt://mqtt.example.com:1883 my-robot --sshd-port 2222

# sshd on a separate machine on the same LAN
magpie-ssh-server-mqtt mqtt://mqtt.example.com:1883 my-robot \
    --sshd-host 192.168.1.50 --sshd-port 22
```

---

## MQTT options (`--mqtt-params`)

Both the server and the client accept `--mqtt-params` to configure broker
authentication, TLS, QoS, and protocol version.  The value is either an inline
JSON object or a path prefixed with `@`:

```bash
--mqtt-params '{"auth": {"mode": "username_password", "username": "u", "password": "p"}}'
--mqtt-params @/etc/magpie/mqtt.json
```

### Supported keys

```json
{
    "protocol_version": 5,
    "auth": {
        "mode": "username_password",
        "username": "myuser",
        "password": "mypassword"
    },
    "tls": {
        "ca_file": "/etc/ssl/certs/broker-ca.pem",
        "verify_peer": true,
        "certfile": "/etc/magpie/client.crt",
        "keyfile":  "/etc/magpie/client.key"
    },
    "defaults": {
        "publish_qos":   1,
        "subscribe_qos": 1
    }
}
```

| Key | Description |
|-----|-------------|
| `protocol_version` | `5` (MQTTv5, default) or `3` / `311` (MQTTv3.1.1) |
| `auth.mode` | `"username_password"` |
| `auth.username` | Broker username |
| `auth.password` | Broker password |
| `tls.ca_file` | CA certificate to verify the broker |
| `tls.verify_peer` | `true` / `false` (default `true`) |
| `tls.certfile` | Client certificate for mutual TLS |
| `tls.keyfile` | Client private key for mutual TLS |
| `defaults.publish_qos` | QoS for outgoing messages (`0`, `1`, or `2`) |
| `defaults.subscribe_qos` | QoS for subscriptions (`0` or `1`) |

---

## Using a cloud MQTT broker (Ably)

[Ably](https://ably.com) is a managed cloud messaging platform that works well
as a relay for `magpie-ssh-mqtt`. It supports MQTT 3.1.1 with TLS and
username/password authentication. Note that Ably only supports QoS 0 for
subscriptions.

### 1. Create an Ably account and get your API key

Your API key has the form `AppId.KeyId:KeySecret`.

### 2. Create a params file

`ably.json`:
```json
{
    "protocol_version": 3,
    "auth": {
        "mode": "username_password",
        "username": "YOUR_APP_ID.YOUR_KEY_ID",
        "password": "YOUR_KEY_SECRET"
    },
    "defaults": {
        "subscribe_qos": 0
    }
}
```

### 3. Start the server (on the robot / remote machine)

```bash
magpie-ssh-server-mqtt mqtts://mqtt.ably.io:8883 my-robot \
    --mqtt-params @/etc/magpie/ably.json
```

### 4. Connect from the client

```bash
magpie-ssh-mqtt mqtts://mqtt.ably.io:8883 my-robot \
    --mqtt-params @ably.json
```

Or with a ProxyCommand in `~/.ssh/config`:

```sshconfig
Host my-robot
    ProxyCommand magpie-ssh-mqtt --proxy mqtts://mqtt.ably.io:8883 %h \
                     --mqtt-params @/path/to/ably.json
    User alice
```

---

## Multiple simultaneous sessions

Multiple SSH clients can connect to the same server at the same time. Each
session gets a unique ULID, its own MQTT topics, and its own `sshd` socket.
They share the broker connection but are completely isolated from each other.

```bash
# Terminal 1
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot

# Terminal 2 (different shell, same server)
magpie-ssh-mqtt mqtt://mqtt.example.com:1883 my-robot

# Both sessions are live simultaneously
```

---

## Troubleshooting

**`no response from server`** — the server is not running, or the broker URI /
`node_id` doesn't match between client and server.

**`cannot reach sshd`** — the server process started but the local `sshd` is
not listening on the expected host/port. Check `--sshd-host` and `--sshd-port`,
and verify `sshd` is running (`systemctl status sshd`).

**Connection drops or very slow** — use a broker with low latency.  Large SSH
payloads (e.g. file transfers with `scp`) generate many small MQTT messages; a
broker with high per-message overhead will feel sluggish. For bulk transfers
`rsync` over the tunnel is more efficient than `scp`.

**Ably / broker rejects subscriptions** — some brokers restrict wildcard
subscriptions. `magpie-ssh-mqtt` uses only specific per-session topics (no
wildcards), so it is compatible with brokers that disallow wildcards.

**Windows `--mqtt-params` quoting** — on Windows, pass the params via a file
(`@C:\path\to\params.json`) rather than inline JSON to avoid shell quoting
issues in the ProxyCommand string.
