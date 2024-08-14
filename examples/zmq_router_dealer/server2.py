import zmq
import threading
import time
import signal
import sys
from srpc import SocketReqRep




class Server:

    def __init__(self):

        self.worker_addr = "inproc://workers"
        self.client_addr = "tcp://127.0.0.1:5555"
        self.stop_event = threading.Event()

        self.c = 0

    def somef(self):
        print('in somef')
        time.sleep(1)
        self.c += 1
        print('out of somef ', self.c)

    def worker_routine(self):
        """Worker routine to handle requests."""
        print('started worker')

        socket = SocketReqRep(zmq_type = 'REP', bind = False, addr = self.worker_addr, shared_context = True)

        # context = context or zmq.Context.instance()
        # socket = context.socket(zmq.REP)
        # socket.connect(worker_url)
        # socket.setsockopt(zmq.LINGER, 0)

        # socket.setsockopt(zmq.RCVTIMEO, 1000)
        # socket.setsockopt(zmq.SNDTIMEO, 1000)

        while not self.stop_event.isSet():
            # Wait for a request from the DEALER
            message = socket.recv()
            # print('message: ', message)
            if message is not None:
                print(f"Worker received request: {message}")

                # Simulate some 'work' being done
                self.somef()

                # Send a reply back to the DEALER
                socket.send(f"Processed: {message}")
        print('out of worker')
        socket.close()

    def serve(self):

        context = zmq.Context.instance() # zmq.Context() # context must be shared

        print(f'Load balancer workers requesting to: {self.worker_addr} ')
        print(f'Load balancer clients requesting to: {self.client_addr} ')

        # Socket to send messages to workers
        backend = context.socket(zmq.DEALER)
        # backend.setsockopt(zmq.LINGER, 0)
        backend.bind(self.worker_addr)

        # Socket to receive messages from clients
        frontend = context.socket(zmq.ROUTER)
        # frontend.setsockopt(zmq.LINGER, 0)
        frontend.bind(self.client_addr)

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
            thread = threading.Thread(target=self.worker_routine)
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
                self.stop_event.set()
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


if __name__ == "__main__":
    server = Server()
    server.serve()
