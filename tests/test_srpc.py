"""Regression tests; all network traffic stays on loopback."""
import json
import queue
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import zmq

from srpc import SRPCClient, SRPCServer, Registry, RegistryClient, RPCError, rpc_method
from srpc.custom_zmq import (
    ZMQServiceBroker, ZMQServiceBrokerService, ZMQServiceBrokerClient, ZMQSub,
    COMM_TYPE_HEARTBEAT,
)
from srpc.store import Store, StoreClient
from srpc.echo import Echo, EchoClient


class ExampleServer(SRPCServer):
    @rpc_method
    def echo(self, value, delay=0):
        time.sleep(delay)
        return value

    @rpc_method
    def fail(self):
        raise ValueError("remote failure")

    @rpc_method
    def unserializable(self):
        return object()

    def local_only(self):
        return "must not be exposed"


class LocalTests(unittest.TestCase):
    def setUp(self):
        self.server = ExampleServer("test", clear_screen=False)
        self.addCleanup(self.server.close)

    def test_explicit_dispatch(self):
        for name in ["local_only", "close", "start", "_close", "__getattribute__"]:
            with self.subTest(name=name):
                self.assertEqual(self.server.handle_request({"method": name})["status"], "error")
        self.assertEqual(self.server.handle_request({"method": "echo", "args": [3]})["output"], 3)

    def test_function_and_class_registration(self):
        self.server.register_function(lambda x: x + 1, "increment")
        class Calculator:
            def double(self, value): return value * 2
            def _private(self): return 9
        self.server.register_class(Calculator)
        self.assertEqual(self.server.handle_request({"method": "Calculator.double", "args": [4]})["output"], 8)
        self.assertEqual(self.server.handle_request({"method": "Calculator._private"})["status"], "error")

    def test_invalid_requests_are_json_errors(self):
        for raw in ["{", "null", "[]", "{}", '{"method": 3}',
                    '{"method": "echo", "args": {}}', '{"method": "echo", "kwargs": []}']:
            with self.subTest(raw=raw):
                self.assertEqual(json.loads(self.server._response_json(raw))["status"], "error")

    def test_nonserializable_result_is_json_error(self):
        self.assertEqual(json.loads(self.server._response_json('{"method":"unserializable"}'))["status"], "error")

    def test_context_ownership_and_idempotent_close(self):
        other = SRPCServer("other", clear_screen=False)
        self.addCleanup(other.close)
        self.server.close()
        self.server.close()
        self.assertFalse(other.ctx.closed)
        ctx = zmq.Context()
        borrowed = SRPCServer("borrowed", clear_screen=False, context=ctx)
        borrowed.close()
        self.assertFalse(ctx.closed)
        ctx.term()

    def test_subscription_timeout_units_and_zero(self):
        with SRPCClient(info=False) as client:
            client.set_timeo(0.25)
            self.assertEqual(client._proxy_sub.timeo, 250)
            client.set_timeo(0)
            self.assertEqual(client._proxy_sub.timeo, 0)
            with patch.object(client._broker_client, "rep", return_value=None) as rep:
                client.receive_any(timeo=0)
                rep.assert_called_once_with(timeo=0)

    def test_close_on_failed_send(self):
        client = SRPCClient(info=False)
        with patch.object(client, "send", return_value=None):
            self.assertEqual(client.invoke("test", "echo", close=True), client.no_req_msg)
        self.assertTrue(client._closed)

    def test_busy_worker_heartbeat_preserves_request(self):
        service = ZMQServiceBrokerService("test")
        service.add_worker("worker")
        service.add_request("client", "request", "payload")
        service.mark_worker_busy(service.next_worker(), service.next_request())
        service.add_worker("worker")
        self.assertIn("worker", service.inflight_by_worker)
        self.assertFalse(service.ready_workers)
        service.inflight_by_worker["worker"].expiry = 0
        self.assertEqual(service.purge()[0][:2], ("client", "request"))

    def test_legacy_registry_listing(self):
        from srpc.registry import SRPCRegistry
        registry = SRPCRegistry.__new__(SRPCRegistry)
        registry.services = {}
        registry.handle_heartbeat({"name": "test", "rep_address": "address"})
        self.assertEqual(registry.list_services()["services"], {"test": "address"})


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.stop = threading.Event()
        self.ready = queue.Queue()
        def run():
            broker = ZMQServiceBroker("tcp://127.0.0.1:*", stop_event=self.stop,
                                      timeo=20, request_max_work_time=0.8)
            self.ready.put(broker.socket.getsockopt_string(zmq.LAST_ENDPOINT))
            broker.serve()
        self.broker_thread = threading.Thread(target=run, daemon=True)
        self.broker_thread.start()
        self.addr = self.ready.get(timeout=5)
        self.servers = []
        self.client = SRPCClient(broker_addr=self.addr, timeo=2, info=False)
        self.addCleanup(self.cleanup)
        self.launch(ExampleServer("test", broker_addr=self.addr, clear_screen=False, n_workers=2))
        self.assertEqual(self.client.invoke("test", "echo", args=["ready"]), "ready")

    def launch(self, server):
        thread = threading.Thread(target=server.serve, daemon=True)
        self.servers.append((server, thread))
        thread.start()
        return server

    def cleanup(self):
        self.client.close()
        for server, thread in self.servers:
            server.close()
            thread.join(3)
            self.assertFalse(thread.is_alive(), "server failed to stop")
        self.stop.set()
        self.broker_thread.join(3)
        self.assertFalse(self.broker_thread.is_alive())

    def test_rpc_and_remote_exception(self):
        self.assertEqual(self.client.call("test", "echo", args=[42])["output"], 42)
        with self.assertRaisesRegex(RPCError, "remote failure"):
            self.client.invoke("test", "fail", raise_errors=True)
        self.assertEqual(self.client.invoke("test", "fail"), "remote failure")
        self.assertEqual(self.client.call("test", "unserializable")["status"], "error")
        self.assertEqual(self.client.invoke("test", "echo", args=["still alive"]), "still alive")

    def test_malformed_request_on_wire(self):
        req_id = self.client._broker_client.req("test", "{", timeo=1)
        response = self.client.receive(req_id)
        self.assertEqual(response["status"], "error")
        self.assertNotIn("Invalid JSON response", response["error_msg"])

    def test_out_of_order_replies_are_retained(self):
        slow = self.client.send("test", "echo", args=["slow"], kwargs={"delay": 0.15})
        fast = self.client.send("test", "echo", args=["fast"])
        self.assertEqual(self.client.receive(slow)["output"], "slow")
        self.assertEqual(self.client.receive(fast)["output"], "fast")

    def test_receive_any_drains_buffer(self):
        slow = self.client.send("test", "echo", args=["slow"], kwargs={"delay": 0.15})
        fast = self.client.send("test", "echo", args=["fast"])
        self.client.receive(slow)
        self.assertEqual(self.client.receive_any(timeo=0), {"req_id": fast, "output": "fast"})

    def test_timeout_does_not_discard_late_response(self):
        self.client.set_timeo(0.02)
        identity = self.client._broker_client.identity
        req_id = self.client.send("test", "echo", args=["late"], kwargs={"delay": 0.15})
        self.assertEqual(self.client.receive(req_id)["status"], "error")
        self.assertEqual(self.client._broker_client.identity, identity)
        self.client.set_timeo(1)
        self.assertEqual(self.client.receive(req_id)["output"], "late")

    def test_missing_service_returns_correlated_error(self):
        response = self.client.call("missing", "echo")
        self.assertEqual(response["status"], "error")
        self.assertIn("expired before dispatch", response["error_msg"])

    def test_registry_round_trip(self):
        registry = self.launch(Registry(broker_addr=self.addr, clear_screen=False))
        with RegistryClient(broker_addr=self.addr, timeo=2, info=False) as client:
            self.assertEqual(client.heartbeat({"name": "test", "rep_address": self.addr}), 1)
            self.assertEqual(client.services()["test"]["rep_address"], self.addr)
            with registry._services_lock:
                registry._services["test"]["last_heartbeat"] = 0
            self.assertEqual(client.services(), {})

    def test_store_and_echo_remain_exposed(self):
        with tempfile.TemporaryDirectory() as directory:
            server = self.launch(Store(broker_addr=self.addr, filename=str(Path(directory) / "store.pkl")))
            store = StoreClient(self.client)
            store.set("a", "b", 2)
            self.assertEqual(store.get("a", "b"), 2)
            self.assertEqual(self.client.call("Store", "read_store")["status"], "error")
            server.close()
            # Remove the already closed server before the temporary directory goes away.
            _, thread = self.servers.pop()
            thread.join(3)
        self.launch(Echo(broker_addr=self.addr, delay=0))
        echo = EchoClient(self.client)
        self.assertEqual(echo.echo("hello"), "hello")
        request = echo.echo("async", collect=False)
        self.assertEqual(echo.collect(), {"req_id": request, "output": "async"})

    def test_worker_loss_times_out(self):
        ctx = zmq.Context()
        worker = ctx.socket(zmq.DEALER)
        worker.setsockopt(zmq.LINGER, 0)
        worker.setsockopt_string(zmq.IDENTITY, "lost-worker")
        worker.connect(self.addr)
        try:
            worker.send_multipart([b"", b"W", b"H", b"lost", b""])
            self.assertTrue(worker.poll(2000))
            worker.recv_multipart()
            req = self.client.send("lost", "echo")
            self.assertTrue(worker.poll(2000))
            worker.recv_multipart()
            worker.close()
            response = self.client.receive(req)
            self.assertEqual(response["status"], "error")
            self.assertIn("timed out while processing", response["error_msg"])
        finally:
            worker.close()
            ctx.term()


class SubscriptionTests(unittest.TestCase):
    def test_multipart_latest_and_reconnect(self):
        ctx = zmq.Context()
        pub = ctx.socket(zmq.XPUB)
        pub.setsockopt(zmq.LINGER, 0)
        pub.bind("tcp://127.0.0.1:*")
        addr = pub.getsockopt_string(zmq.LAST_ENDPOINT)
        client = SRPCClient(proxy_pub_addr=addr, info=False)
        try:
            client.subscribe("topic")
            self.assertTrue(pub.poll(2000))
            self.assertEqual(pub.recv(), b"\x01topic")
            for i in range(10): pub.send_multipart([b"topic", str(i).encode()])
            # Wait for the local burst to arrive before asking for the latest.
            time.sleep(0.05)
            self.assertEqual(client.listen(), ("topic", "9"))
            client.reconnect()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if pub.poll(100) and pub.recv() == b"\x01topic": break
            else: self.fail("subscription not restored")
            pub.send_multipart([b"topic", b"restored"])
            self.assertEqual(client.listen(), ("topic", "restored"))
            client.unsubscribe("topic")
            self.assertNotIn("topic", client._subscriptions)
        finally:
            client.close()
            pub.close()
            ctx.term()


if __name__ == "__main__":
    unittest.main()
