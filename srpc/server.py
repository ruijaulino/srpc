"""Explicitly registered JSON RPC services."""
import json
import threading

import zmq

from .custom_zmq import ZMQPub, ZMQServiceBrokerWorker
from .defaults import BROKER_ADDR, PROXY_SUB_ADDR
from .utils import build_server_response, OK_STATUS, ERROR_STATUS, clear_screen


def rpc_method(func):
    """Mark a public instance method as remotely callable."""
    func._srpc_exposed = True
    return func


def _public_name(name):
    return isinstance(name, str) and bool(name) and all(
        part.isidentifier() and not part.startswith("_") for part in name.split(".")
    )


class SRPCServer:
    def __init__(self, name, broker_addr=None, proxy_sub_addr=None, timeo=1,
                 n_workers=1, thread_safe=False, clear_screen=True,
                 worker_info=False, context=None):
        if n_workers < 1:
            raise ValueError("n_workers must be positive")
        self._name = name
        self._broker_addr = broker_addr or BROKER_ADDR
        self._proxy_sub_addr = proxy_sub_addr or PROXY_SUB_ADDR
        self._timeo = timeo
        self._clear_screen = clear_screen
        self._owns_context = context is None
        self.ctx = zmq.Context() if context is None else context
        self.stop_event = threading.Event()
        self._pub_socket = ZMQPub(ctx=self.ctx, timeo=timeo)
        self._pub_socket.connect(self._proxy_sub_addr)
        self._functions = {}
        self._classes = {}
        self._class_instances = {}
        self._thread_safe_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._thread_safe = thread_safe
        self._n_workers = n_workers
        self._worker_info = worker_info
        self._workers = []
        self._thread_names = {}
        self._closed = False
        self._serving = False
        # Inspect the class, avoiding evaluation of arbitrary instance properties.
        for attr in dir(type(self)):
            method = getattr(type(self), attr, None)
            if callable(method) and getattr(method, "_srpc_exposed", False):
                self.register_function(getattr(self, attr), attr)

    @rpc_method
    def publish(self, topic, msg):
        with self._publish_lock:
            self._pub_socket.publish(str(topic), str(msg))

    def register_function(self, func, name=None):
        name = name or func.__name__
        if not _public_name(name) or not callable(func):
            raise ValueError("RPC functions require a public name and a callable")
        self._functions[name] = func

    def register_class(self, cls, name=None, methods=None):
        """Register public class methods, or an explicit list of method names."""
        name = name or cls.__name__
        if not _public_name(name):
            raise ValueError("RPC classes require a public name")
        instance = cls()
        names = methods if methods is not None else [
            attr for attr in dir(cls)
            if _public_name(attr) and callable(getattr(cls, attr, None))
        ]
        for attr in names:
            if not _public_name(attr) or "." in attr:
                raise ValueError("RPC methods require public names")
            self.register_function(getattr(instance, attr), name + "." + attr)
        self._classes[name] = cls
        self._class_instances[name] = instance

    def handle_request(self, request):
        try:
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            method = request.get("method")
            args = request.get("args", [])
            kwargs = request.get("kwargs", {})
            if not _public_name(method):
                raise ValueError("method must be a public RPC name")
            if not isinstance(args, list):
                raise ValueError("args must be a list")
            if not isinstance(kwargs, dict) or not all(isinstance(k, str) for k in kwargs):
                raise ValueError("kwargs must be an object with string keys")
            if method not in self._functions:
                raise ValueError(f"Unknown method: {method}")
            output = self._functions[method](*args, **kwargs)
            return build_server_response(OK_STATUS, output, "")
        except Exception as exc:
            return build_server_response(ERROR_STATUS, None, str(exc))

    def _response_json(self, raw):
        """Every worker response, including failures, uses the same envelope."""
        try:
            response = self.handle_request(json.loads(raw))
            return json.dumps(response)
        except Exception as exc:
            return json.dumps(build_server_response(ERROR_STATUS, None, str(exc)))

    def worker_print(self, msg):
        if self._worker_info:
            print(f"[{self._name}] {msg}")

    def base_worker(self):
        socket = ZMQServiceBrokerWorker(self.ctx, self._name, self._worker_info)
        try:
            socket.connect(self._broker_addr)
            self._thread_names[threading.get_ident()] = socket.identity
            while not self.stop_event.is_set():
                client_id, req_id, raw = socket.recv_work()
                if raw is None:
                    continue
                if self._thread_safe:
                    with self._thread_safe_lock:
                        response = self._response_json(raw)
                else:
                    response = self._response_json(raw)
                socket.send_work(client_id, req_id, response)
        finally:
            socket.close()
            self._thread_names.pop(threading.get_ident(), None)

    def _serve(self):
        try:
            with self._close_lock:
                if self._closed or self._serving:
                    raise RuntimeError("server is closed or already serving")
                self._serving = True
                for _ in range(self._n_workers):
                    worker = threading.Thread(target=self.base_worker)
                    self._workers.append(worker)
                    worker.start()
            self.stop_event.wait()
        except KeyboardInterrupt:
            self.stop_event.set()
        finally:
            self.close()

    def serve(self):
        if self._clear_screen:
            clear_screen()
            self._clear_screen = False
        try:
            self.start()
            self._serve()
        finally:
            self.close()

    def _close(self):
        # Workers may request shutdown, but must not join themselves or terminate
        # the context before their reply socket has closed.
        self.stop_event.set()
        if threading.current_thread() in self._workers:
            return
        with self._close_lock:
            if self._closed:
                return
            for worker in self._workers:
                worker.join()
            with self._publish_lock:
                self._pub_socket.close()
            if self._owns_context:
                self.ctx.term()
            self._closed = True

    def close(self):
        self._close()

    def start(self):
        """Override for local initialization before serving."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
