import time
import zmq
import threading

LRU_READY = b"\x01"

WORKER_READY = b"\x01"

# Reliable load balancer
class ZMQQueue:
    def __init__(self, ctx:zmq.Context, client_addr:str, worker_addr:str, poll_timeo:int = 1, standalone:bool = False):
        self.ctx = ctx
        self.client_addr = client_addr
        self.worker_addr = worker_addr
        self.poll_timeo = 1000*poll_timeo
        self.standalone = standalone
        self._stopevent = threading.Event()
        self.th = None

    def _condition(self):
        if self.standalone:
            return True
        else:
            return not self._stopevent.isSet()

    def queue(self):

        frontend = self.ctx.socket(zmq.ROUTER) # ROUTER
        backend = self.ctx.socket(zmq.ROUTER) # ROUTER
        
        frontend.bind(self.client_addr) # For clients
        backend.bind(self.worker_addr)  # For workers

        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)

        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)

        workers = []

        while self._condition():
            try:
                if workers:
                    socks = dict(poll_both.poll(self.poll_timeo))
                else:
                    socks = dict(poll_workers.poll(self.poll_timeo))
                # Handle worker activity on backend
                if socks.get(backend) == zmq.POLLIN:
                    # Use worker address for LRU routing
                    msg = backend.recv_multipart()
                    address = msg[0]
                    workers.append(address)
                    # Everything after the second (delimiter) frame is reply
                    reply = msg[2:]
                    # Forward message to client if it's not a READY
                    if reply[0] != WORKER_READY:
                        frontend.send_multipart(reply)

                if socks.get(frontend) == zmq.POLLIN:
                    #  Get client request, route to first available worker
                    msg = frontend.recv_multipart()
                    request = [workers.pop(0), ''.encode()] + msg
                    backend.send_multipart(request)

            except Exception as e:
                print('ZMQQueue thread error : ', e)
                if self.standalone: 
                    break

        frontend.close()
        backend.close()

    def stop(self):
        # closing the sockets will trigger an error inside the thread
        # stopping the proxy        
        self._stopevent.set()
        if self.th: self.th.join()
        
    def start(self):
        if self.standalone:
            self.queue()
        else:
            # Start the proxy in a separate thread
            self.th = threading.Thread(target=self.queue)
            self.th.start()              


ctx = zmq.Context()
q = ZMQQueue(ctx, "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556", standalone = True)
q.start()

exit(0)

while True:
    try:
        time.sleep(0.1)
    except KeyboardInterrupt:
        print('detected KeyboardInterrupt')
        break
q.stop() 
ctx.term()       

exit(0)

context = zmq.Context(1)

frontend = context.socket(zmq.ROUTER) # ROUTER
backend = context.socket(zmq.ROUTER) # ROUTER
frontend.bind("tcp://127.0.0.1:5555") # For clients
backend.bind("tcp://127.0.0.1:5556")  # For workers

poll_workers = zmq.Poller()
poll_workers.register(backend, zmq.POLLIN)

poll_both = zmq.Poller()
poll_both.register(frontend, zmq.POLLIN)
poll_both.register(backend, zmq.POLLIN)

workers = []
poll_timeo = 1000
while True:

    try:

        if workers:
            socks = dict(poll_both.poll(poll_timeo))
        else:
            socks = dict(poll_workers.poll(poll_timeo))

        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            msg = backend.recv_multipart()
            print('backend msg: ', msg)
            if not msg:
                break
            address = msg[0]
            workers.append(address)
            print('workers len ', len(workers))
            # Everything after the second (delimiter) frame is reply
            reply = msg[2:]
            print('reply: ', reply)
            # Forward message to client if it's not a READY
            if reply[0] != LRU_READY:
                print('backend send to frontend: ', reply)
                frontend.send_multipart(reply)

        if socks.get(frontend) == zmq.POLLIN:
            #  Get client request, route to first available worker
            msg = frontend.recv_multipart()
            print('frontend msg: ', msg)
            request = [workers.pop(0), ''.encode()] + msg
            print('frontend send to backend: ', request)
            backend.send_multipart(request)
    except KeyboardInterrupt:
        print('detected KeyboardInterrupt')
        break

frontend.close()
backend.close()
context.term()