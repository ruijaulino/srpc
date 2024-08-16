import zmq
import threading
import time
import signal
import sys
from srpc import SocketReqRep

def worker_routine(worker_url, stop_event):
    """Worker routine to handle requests."""
    print('started worker')

    socket = SocketReqRep(zmq_type = 'REP', bind = False, addr = worker_url, shared_context = True)

    # context = context or zmq.Context.instance()
    # socket = context.socket(zmq.REP)
    # socket.connect(worker_url)
    # socket.setsockopt(zmq.LINGER, 0)

    # socket.setsockopt(zmq.RCVTIMEO, 1000)
    # socket.setsockopt(zmq.SNDTIMEO, 1000)

    while not stop_event.isSet():
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

    worker_addr = "inproc://workers"
    client_addr = "tcp://127.0.0.1:5555"


    stop_event = threading.Event()

    context = zmq.Context.instance() # zmq.Context() # context must be shared

    print(f'Load balancer workers requesting to: {worker_addr} ')
    print(f'Load balancer clients requesting to: {client_addr} ')

    # Socket to send messages to workers
    backend = context.socket(zmq.DEALER)
    # backend.setsockopt(zmq.LINGER, 0)
    backend.bind(worker_addr)

    # Socket to receive messages from clients
    frontend = context.socket(zmq.ROUTER)
    # frontend.setsockopt(zmq.LINGER, 0)
    frontend.bind(client_addr)

    # # Flag to indicate if we should shut down
    # shutdown_flag = threading.Event()

    # # Function to handle shutdown
    # def shutdown(signum, frame):
    #     print("Shutting down...")
    #     frontend.close()
    #     backend.close()
    #     context.term()
    #     sys.exit(0)

    time.sleep(1)
    print('create workers')



    workers = []
    for _ in range(3):  # Number of worker threads
        thread = threading.Thread(target=worker_routine, args=(worker_addr,stop_event,))
        thread.start()
        workers.append(thread)

    def proxy(frontend, backend):
        try:
            zmq.proxy(frontend, backend)
        except zmq.error.ZMQError:
            pass
        except Exception as e:
            print(f"Unexpected error: {e}")

    # Start the proxy in a separate thread
    proxy_thread = threading.Thread(target=proxy, args = (frontend, backend))
    proxy_thread.start()

    while True:
        try:
            time.sleep(0.5)
            pass
        except KeyboardInterrupt:
            stop_event.set()
            print('close frontend')
            frontend.close()
            # time.sleep(1)
            print('close backend')
            backend.close()
            # time.sleep(1)
            print('context term')
            context.term()    
            # time.sleep(1)
            print('join thread!')        
            proxy_thread.join()
            print('done')
            break

    for worker in workers:
        worker.join()



    ## Wait for the shutdown flag to be set
    #while not shutdown_flag.is_set():
    #    try:
    #        shutdown_flag.wait(1)            
    #        # signal.pause()  # Wait for signals
    #    except KeyboardInterrupt:
    #        shutdown(None, None)

    # sys.exit(0)


    # # Prepare context and sockets
    # context = zmq.Context.instance()
    
    # # Socket to talk to clients
    # frontend = context.socket(zmq.ROUTER)
    # frontend.bind("tcp://*:5555")

    # # Socket to talk to workers
    # backend = context.socket(zmq.DEALER)
    # backend.bind("inproc://workers")

    # # Launch worker threads
    # #workers = []
    # #for _ in range(3):  # Number of worker threads
    # #    thread = threading.Thread(target=worker_routine, args=("inproc://workers",))
    # #    thread.start()
    # #    workers.append(thread)

    # # Use zmq.proxy to connect frontend and backend
    # try:
    #     # Use zmq.proxy to connect frontend and backend
    #     zmq.proxy(frontend, backend)
    # except zmq.ZMQError:
    #     # If we get an error here, it's likely because of the Ctrl-C stop
    #     print('ZMQError')
    #     pass
    # finally:
    #     # Clean up
    #     frontend.close()
    #     backend.close()
    #     context.term()
        
    #     # Wait for all workers to finish
    #     for worker in workers:
    #         worker.join()


if __name__ == "__main__":
    main()
