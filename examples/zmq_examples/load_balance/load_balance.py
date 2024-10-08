from __future__ import print_function

import multiprocessing
import threading
import zmq


NBR_CLIENTS = 10
NBR_WORKERS = 3


frontend_addr = "inproc://frontend"
frontend_addr = "tcp://127.0.0.1:5671"
backend_addr = "inproc://backend"
backend_addr = "tcp://127.0.0.1:5672"

def client_task(ident):
    """Basic request-reply client using REQ socket."""
    # print('starting client')
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect(frontend_addr)

    # Send request, get reply
    socket.send_string("HELLO")
    reply = socket.recv()
    print("IN CLIENT {}: {}".format(socket.identity.decode("ascii"), reply.decode("ascii")))


def worker_task(ident):
    """Worker task, using a REQ socket to do load-balancing."""
    # print('starting worker')
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect(backend_addr)

    # Tell broker we're ready for work
    socket.send(b"READY")

    while True:
        address, empty, request = socket.recv_multipart()
        print("IN WORKER {} FROM {}: {}".format(socket.identity.decode("ascii"), address.decode('ascii'),request.decode("ascii")))
        socket.send_multipart([address, b"", b"OK"])


def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(frontend_addr)
    backend = context.socket(zmq.ROUTER)
    backend.bind(backend_addr)

    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()
    for i in range(NBR_CLIENTS):
        start(client_task, i)
    for i in range(NBR_WORKERS):
        start(worker_task, i)

    # Initialize main loop state
    count = NBR_CLIENTS
    backend_ready = False
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())
        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            worker, empty, client = request[:3]
            workers.append(worker)
            if workers and not backend_ready:
                # Poll for clients now that a worker is available and backend was not ready
                poller.register(frontend, zmq.POLLIN)
                backend_ready = True
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
                count -= 1
                if not count:
                    break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            client, empty, request = frontend.recv_multipart()
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available and set backend_ready flag to false
                poller.unregister(frontend)
                backend_ready = False

    # Clean up
    backend.close()
    frontend.close()
    context.term()


if __name__ == "__main__":
    main()