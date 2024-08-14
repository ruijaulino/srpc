import zmq
import threading
import time
from srpc import SocketReqRep

def worker_routine(worker_url, context=None):
    """Worker routine to handle requests."""
    context = zmq.Context() # context or zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.connect(worker_url)

    while True:
        # Wait for a request from the DEALER
        message = socket.recv_string()
        print(f"Worker received request: {message}")

        # Simulate some 'work' being done
        time.sleep(2)

        # Send a reply back to the DEALER
        socket.send_string(f"Processed: {message}")

def worker_routine(worker_url, stop_event = None):
    """Worker routine to handle requests."""
    print('started worker')

    socket = SocketReqRep(zmq_type = 'REP', bind = False, addr = worker_url)

    # context = context or zmq.Context.instance()
    # socket = context.socket(zmq.REP)
    # socket.connect(worker_url)
    # socket.setsockopt(zmq.LINGER, 0)

    # socket.setsockopt(zmq.RCVTIMEO, 1000)
    # socket.setsockopt(zmq.SNDTIMEO, 1000)

    while True:
        # Wait for a request from the DEALER
        message = socket.recv()
        # print('message: ', message)
        if message is not None:
            print(f"Worker received request: {message}")

            # Simulate some 'work' being done
            time.sleep(2)

            # Send a reply back to the DEALER
            socket.send(f"Processed: {message}")
    print('out of worker')
    socket.close()

def main():
    # Prepare context and sockets
    context = zmq.Context.instance()
    
    # Socket to talk to clients
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://127.0.0.1:5555")

    # Socket to talk to workers
    backend = context.socket(zmq.DEALER)
    backend.bind("inproc://workers")

    # Launch worker threads
    for _ in range(3):  # Number of worker threads
        thread = threading.Thread(target=worker_routine, args=("inproc://workers",))
        thread.start()

    # Use zmq.proxy to connect frontend and backend
    zmq.proxy(frontend, backend)

if __name__ == "__main__":
    main()
