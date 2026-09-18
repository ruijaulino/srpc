# srpc

A small JSON RPC framework built on ZeroMQ, with named services, worker pools,
and topic subscriptions. Python 3.8 or newer is required.

## Installation

```sh
python -m pip install .
```

Runtime dependencies are installed automatically. For development, use
`python -m pip install -e .`.

## Quick start

Start the broker and publication proxy in one terminal:

```sh
python -c "from srpc import devices; devices()"
```

Start a server in another terminal:

```python
from srpc import SRPCServer, rpc_method

class Calculator(SRPCServer):
    @rpc_method
    def add(self, a, b):
        return a + b

Calculator("calculator", clear_screen=False).serve()
```

Call it from a client:

```python
from srpc import SRPCClient

with SRPCClient(timeo=2, info=False) as client:
    result = client.invoke("calculator", "add", args=[2, 3], raise_errors=True)
    print(result)  # 5
```

Alternatively run `python examples/simple_functions/server.py` and
`python examples/simple_functions/client.py` after installing the package.

The defaults are loopback TCP ports 4002 (broker), 4003 (subscribers connect),
and 4004 (publishers connect). Configure `broker_addr`, `proxy_pub_addr` on
clients, and `proxy_sub_addr` on servers for other endpoints.

## Registering methods

Only explicitly exposed methods are callable:

- Decorate server subclass methods with `@rpc_method`.
- Use `server.register_function(function, name="optional_alias")`.
- Use `server.register_class(Calculator)` to expose its public callable methods
  as `Calculator.method`, or pass `methods=["add"]` to select a subset.

Private names are rejected. Lifecycle and persistence helpers remain local.
The built-in `publish` method and the Store, Echo, and Registry service APIs
are explicitly exposed. Register methods before calling `serve()`.

Requests and results must be JSON serializable. Requests use an object with
`method` (string), `args` (list), and `kwargs` (object). Invalid requests,
method exceptions, and unserializable results return a JSON error envelope.

## Results and errors

`invoke()` returns the result on success and error text on failure for
compatibility. `invoque()` remains an alias. Use `raise_errors=True` to raise
`RPCError`, whose `response` attribute contains the complete envelope.

`call()` and `receive()` return structured responses:

```python
{"status": "ok", "output": 5, "error_msg": ""}
{"status": "error", "output": None, "error_msg": "Unknown method: missing"}
```

For multiple outstanding requests:

```python
first = client.send("calculator", "add", args=[1, 2])
second = client.send("calculator", "add", args=[3, 4])
print(client.receive(first))
print(client.receive(second))
```

Replies arriving out of order are buffered by request ID. `receive_any()`
returns `{"req_id": ..., "output": ...}` and consumes buffered replies first.
Collect all outstanding replies to release their buffered storage.

Public client timeouts are in seconds; zero means nonblocking polling.
`set_timeo()` updates both RPC and subscription waits. A receive timeout does
not cancel server execution or reset the connection: the same request can be
received again. Explicit `reconnect()` discards outstanding replies and restores
subscriptions. Do not automatically retry non-idempotent calls after a timeout.

## Publications

```python
client.subscribe("prices.")
# From a server: server.publish("prices.example", "123")
topic, message = client.listen()
client.unsubscribe("prices.")
```

Subscriptions match topic prefixes. With `last_msg_only=True`, the client drains
up to 10,000 additional complete messages and returns the last one encountered,
across all subscribed topics. Use `last_msg_only=False` to receive each queued
message, especially when subscribing to multiple event topics. This is a bounded
queue drain, not a durable latest-value store; ZeroMQ queue limits still apply.

## Registry and shutdown

The broker routes by service name without a registry. The optional directory is
started with `Registry(broker_addr=...).serve()`. `RegistryClient` accepts the
same broker address and provides `heartbeat({"name": ...})` and `services()`.
Send heartbeats periodically; entries expire after twice `REGISTRY_HEARTBEAT`.
Services do not automatically send directory heartbeats.

`SRPCRegistry` remains the legacy direct REQ/REP directory; it uses a different
protocol and is not paired with the broker-backed `RegistryClient`.

Servers own their contexts unless passed `context=...`; caller-owned contexts
are never terminated by the server. `close()` stops workers, waits for running
methods, and releases owned resources. It is safe to call repeatedly. Application
methods must return for graceful shutdown to complete. Stop and join custom
background publishers before calling the base `close()`.

Use a separate client per thread. `thread_safe=True` serializes method execution
within a server; it does not make client sockets safe for concurrent callers.
The transport does not provide authentication or encryption; deploy it on a
trusted network or behind an appropriate authenticated transport.

## Migration notes

Previously, arbitrary server attributes could be invoked remotely. Add
`@rpc_method` to intended subclass endpoints. Registry constructors now accept
`broker_addr` instead of the obsolete `rep_addr` / `req_addr` arguments. Use
`send()` and `receive_any()` for asynchronous calls instead of the removed
`collect` argument to `invoque()`.

## Tests

```sh
python -m unittest discover -s tests -v
python -m pip check
```

Tests use local sockets and cover dispatch restrictions, malformed requests,
serialization failures, out-of-order replies, timeouts, worker loss, registry
expiration, subscriptions, and context ownership. CI runs the suite on Windows
and Linux with Python 3.8 and 3.12, and imports an installed wheel outside the
checkout.
