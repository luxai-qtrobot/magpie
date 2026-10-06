# MAGPIE public WebRTC reference deployment

This directory contains a small, open-source deployment for testing MAGPIE
WebRTC across networks:

- Coturn runs on an Ubuntu VM and relays traffic only when direct WebRTC
  connectivity fails.
- A standalone FastAPI container provides MAGPIE's opaque HTTP signaling
  protocol and short-lived Coturn credentials.
- One caller-chosen `session_id` identifies the signaling room and the
  corresponding temporary TURN user. There is no session-creation or invite
  API.

The signaling service does not import MAGPIE and never carries application
data after the WebRTC links are established.

## Layout

```text
azure-webrtc/
├── signaling/                    # Azure Container Apps source directory
├── coturn/turnserver.conf.example
└── examples/                     # Two Python end-to-end smoke-test peers
```

## 1. Generate the shared TURN secret

Generate one random secret and keep it outside source control. In PowerShell:

```powershell
$bytes = [byte[]]::new(32)
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$rng.Dispose()
$TurnSecret = [Convert]::ToBase64String($bytes)
```

The exact same value must be installed on the Coturn VM and stored as an
Azure Container App secret. The signaling service signs temporary credentials
locally; it never opens a management connection to Coturn.

## 2. Configure Coturn on the Ubuntu VM

Install Coturn and copy
[`coturn/turnserver.conf.example`](coturn/turnserver.conf.example) to
`/etc/turnserver.conf`. Replace:

- `REPLACE_AZURE_PUBLIC_IP` with the VM's static public IP.
- `REPLACE_AZURE_PRIVATE_IP` with the VM NIC's private IP.
- `REPLACE_WITH_THE_SAME_SECRET_USED_BY_THE_CONTAINER_APP` with
  `$TurnSecret` from the previous step.

Enable and restart the service:

```bash
sudo apt update
sudo apt install coturn
sudo systemctl enable coturn
sudo systemctl restart coturn
sudo systemctl status coturn
```

Some Ubuntu Coturn packages also require `TURNSERVER_ENABLED=1` in
`/etc/default/coturn`.

Open these inbound ports in both the Azure network security group and the VM
firewall:

| Protocol | Ports | Purpose |
|---|---:|---|
| UDP and TCP | `3478` | STUN/TURN listener |
| UDP | `49160-49250` | TURN relay allocations |

The template intentionally uses `turn:` with a public IP. Add an Azure DNS
label, a matching certificate, and Coturn TLS configuration before publishing
a `turns:` URL.

## 3. Deploy signaling to Azure Container Apps

Run from the MAGPIE repository root. Install or update the Azure CLI Container
Apps extension first:

```powershell
az login
az extension add --name containerapp --upgrade
az provider register --namespace Microsoft.App
az provider register --namespace Microsoft.OperationalInsights
```

Choose names and enter the Coturn public IP:

```powershell
$ResourceGroup = "magpie-playground-rg"
$Location = "westeurope"
$Environment = "magpie-playground-env"
$App = "magpie-signaling"
$TurnPublicIp = "REPLACE_PUBLIC_IP"
```

Build and deploy the self-contained signaling directory. `az containerapp up`
creates the required Container Apps environment and registry when necessary:

```powershell
az containerapp up `
  --name $App `
  --source .\deploy\azure-webrtc\signaling `
  --resource-group $ResourceGroup `
  --location $Location `
  --environment $Environment `
  --ingress external `
  --target-port 8000 `
  --env-vars `
    "TURN_URLS=turn:${TurnPublicIp}:3478?transport=udp" `
    "STUN_URLS=stun:${TurnPublicIp}:3478" `
    "MAGPIE_SIGNAL_ALLOWED_ORIGINS=https://magpie.luxai.com"
```

Store the shared value as an Azure secret and reference it from the container.
Keep exactly one replica because signaling rooms are held in memory:

```powershell
az containerapp secret set `
  --name $App `
  --resource-group $ResourceGroup `
  --secrets "turn-auth-secret=$TurnSecret"

az containerapp update `
  --name $App `
  --resource-group $ResourceGroup `
  --set-env-vars "TURN_SHARED_SECRET=secretref:turn-auth-secret" `
  --min-replicas 1 `
  --max-replicas 1
```

`min-replicas 1` avoids cold starts and keeps the in-memory relay available.
For a low-use experimental deployment it can be changed to `0`; active peers
must tolerate a restart or scale-down losing signaling state.

Retrieve the generated HTTPS address:

```powershell
$HostName = az containerapp show `
  --name $App `
  --resource-group $ResourceGroup `
  --query properties.configuration.ingress.fqdn `
  --output tsv
$Server = "https://$HostName"
Write-Host $Server
```

## 4. Check the service

```powershell
Invoke-RestMethod "$Server/healthz"
Invoke-RestMethod "$Server/ice/test-$([guid]::NewGuid().ToString('N'))"
```

`/ice/{session_id}` returns this shape with a `Cache-Control: no-store` header:

```json
{
  "expiresAt": 1791209400,
  "stunServers": ["stun:203.0.113.10:3478"],
  "turnServers": [
    {
      "url": "turn:203.0.113.10:3478?transport=udp",
      "username": "1791209400:a84c18f2a73d83e57c8521b2",
      "credential": "short-lived-base64-hmac"
    }
  ]
}
```

The username is an expiration timestamp plus a hash of the MAGPIE session ID.
Coturn recreates the credential using its copy of the shared secret. Peers in
one session reuse the temporary username so Coturn's `user-quota` applies to
the session.

## 5. Force an end-to-end TURN test

Install MAGPIE's WebRTC dependencies in the Python environment:

```powershell
pip install -e ".[webrtc]"
```

Choose one unique ID and use it in both terminals. Start the writer first:

```powershell
$Session = "test-$([guid]::NewGuid().ToString('N'))"
python deploy\azure-webrtc\examples\python_writer.py `
  --server $Server --session $Session --force-turn
```

Then run the reader in a second terminal with the same `$Server` and `$Session`:

```powershell
$Server = "https://REPLACE_WITH_THE_CONTAINER_APP_FQDN"
$Session = "REPLACE_WITH_THE_WRITER_SESSION_ID"
python deploy\azure-webrtc\examples\python_reader.py `
  --server $Server --session $Session --force-turn
```

`--force-turn` sets WebRTC's ICE policy to `relay`. Receiving all ten messages
therefore verifies HTTP signaling, temporary credential generation, Coturn
authentication, and relayed application traffic. Remove the flag to test the
normal direct-first behavior.

## Public endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness and TURN-configuration status |
| `GET /ice/{session_id}` | Short-lived STUN/TURN configuration |
| `/signal/sessions/{session_id}/peers/...` | MAGPIE HTTP signaling contract |

Session and peer IDs accept 3-128 letters, numbers, `.`, `_`, `~`, or `-`.
Choose an unguessable ID for a public relay. Reusing another person's ID has
the same consequence as reusing their topic on a public MQTT broker.

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `TURN_SHARED_SECRET` | unset | Shared Coturn REST authentication secret |
| `TURN_URLS` | unset | Comma-separated `turn:`/`turns:` URLs |
| `STUN_URLS` | unset | Comma-separated `stun:` URLs |
| `TURN_CREDENTIAL_TTL_SECONDS` | `600` | Temporary credential lifetime |
| `MAGPIE_SIGNAL_ALLOWED_ORIGINS` | `https://magpie.luxai.com` | Browser origins allowed by CORS |
| `MAGPIE_MAX_SESSIONS` | `16` | Concurrent in-memory signaling rooms |
| `MAGPIE_MAX_PEERS_PER_SESSION` | `4` | Peers admitted to one room |
| `MAGPIE_PEER_LEASE_SECONDS` | `90` | Inactive-peer expiry |
| `MAGPIE_MAX_MESSAGE_BYTES` | `262144` | Maximum opaque signaling message size |
| `MAGPIE_MAX_QUEUED_MESSAGES` | `256` | Per-peer signaling queue bound |
| `MAGPIE_REQUESTS_PER_MINUTE_PER_IP` | `300` | General per-IP request limit |
| `MAGPIE_ICE_REQUESTS_PER_MINUTE_PER_IP` | `30` | Credential requests per IP |

The limits are intentionally conservative for a small public demonstration.
CORS is a browser control, not authentication. The ICE endpoint remains public,
so Azure traffic metrics and Coturn allocation/bandwidth metrics should be
monitored before increasing capacity.

## Local signaling test

The container can also run locally while using the remote Coturn VM:

```powershell
Copy-Item deploy\azure-webrtc\signaling\.env.example .env
# Edit .env with the Coturn IP and the same shared secret.
docker build -t magpie-signaling deploy\azure-webrtc\signaling
docker run --rm --env-file .env -p 8000:8000 magpie-signaling
```

Use `http://127.0.0.1:8000` as the smoke-test `--server` value.
