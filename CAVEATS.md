# Caveats

Platform-specific behaviors and edge cases to be aware of when using Magpie.

---

## ZMQ DEALER socket: TCP self-connect on Linux loopback

**Affected component:** `ZMQRpcRequester` (`luxai/magpie/transport/zmq/zmq_rpc_requester.py`)

### Symptom

When a remote ZMQ service is stopped and then restarted, the restart fails with
`EADDRINUSE` on the service's bind port — even though no other intentional server
is listening on that port.

Running `ss -tanp | grep :<port>` reveals an `ESTAB` socket owned by the
**client** process with identical source and destination
(`127.0.0.1:<port> → 127.0.0.1:<port>`), locking the port.

### Root cause

This is a Linux TCP self-connect triggered by ZMQ's automatic reconnection:

1. The remote service is stopped — its `LISTEN` socket on port *P* is released.
   Port *P* re-enters the kernel's available ephemeral range (Linux default:
   `32768–60999`).
2. ZMQ's reconnect logic immediately fires a new `connect("tcp://127.0.0.1:P")`
   on the `DEALER` socket.
3. The kernel assigns an ephemeral source port for the outgoing connection. Since
   port *P* was just freed and is the next candidate in the allocation scan, it
   picks *P* as the source port.
4. The outgoing SYN has `src=127.0.0.1:P → dst=127.0.0.1:P`. On the loopback
   interface the packet is delivered back to the same socket. Linux's TCP stack
   treats this as a *simultaneous open* (RFC 793 §3.4) and transitions the socket
   to `ESTABLISHED` **without any server accepting the connection**.
5. The `DEALER` socket now holds port *P* in `ESTAB` state. Any subsequent
   `bind("tcp://*:P")` by the restarted service fails with `EADDRINUSE`.

### Workaround (operational)

Restart the client process before restarting the remote service. This closes the
fd holding the self-connected socket and releases the port.

### Suggested fix (code)

Pre-bind the `DEALER` socket to a random local port **before** calling
`connect()`. This locks in a kernel-chosen ephemeral port that will never equal
the destination port, making the self-connect impossible.

In `zmq_rpc_requester.py`, after creating the socket and before `connect()`:

```python
self.socket = self.context.socket(zmq.DEALER)

if identity is not None:
    self.socket.setsockopt(zmq.IDENTITY, identity)

# Pre-bind to a random local port to prevent TCP self-connect on loopback.
# Without this, ZMQ's reconnect can pick the target port as ephemeral source,
# causing Linux to form a self-loop (ESTAB without a server) that blocks the
# remote service from rebinding its port.
if endpoint.startswith("tcp://127.") or endpoint.startswith("tcp://localhost"):
    self.socket.bind_to_random_port("tcp://127.0.0.1")

self.socket.connect(endpoint)
```

`zmq.Socket.bind_to_random_port()` asks the kernel to bind to an OS-chosen
ephemeral port, ensuring the source port is always distinct from the destination.
