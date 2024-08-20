import zmq
import threading
import time

class Proxy:
    def __init__(self, worker_addr:str, client_addr:str):
        self.worker_addr = worker_addr
        self.client_addr = client_addr
        
        self.proxy_stop_even = threading.Event()

        self.polltimeo = 100

    # run the proxy
    def _proxy(self):   
        context = zmq.Context.instance()  # zmq.Context() 

        frontend = context.socket(zmq.ROUTER)
        backend = context.socket(zmq.DEALER)

        frontend.bind(self.client_addr)
        backend.bind(self.worker_addr)

        frontend.setsockopt(zmq.RCVTIMEO, 1000)
        frontend.setsockopt(zmq.SNDTIMEO, 1000)
        frontend.setsockopt(zmq.LINGER, 0)

        backend.setsockopt(zmq.RCVTIMEO, 1000)
        backend.setsockopt(zmq.SNDTIMEO, 1000)
        backend.setsockopt(zmq.LINGER, 0)

        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(backend, zmq.POLLIN)

        while not self.proxy_stop_even.isSet():
            try:
                if frontend.closed:
                    print('proxy frontend is closed!')
                    break
                if backend.closed:
                    print('proxy backend is closed!')
                    break
                socks = dict(poller.poll(self.polltimeo))
                if socks.get(frontend) == zmq.POLLIN:
                    message = frontend.recv_multipart()
                    backend.send_multipart(message)
                
                if socks.get(backend) == zmq.POLLIN:
                    message = backend.recv_multipart()
                    frontend.send_multipart(message)
                # frontend.close()
            except Exception as e:
                print('error: ', e)
                
        print('closing proxy')
        frontend.close()
        backend.close()
        context.term()
        print('proxy closed')


    def stop(self):
        # closing the sockets will trigger an error inside the thread
        # stopping the proxy
        self.proxy_stop_even.set()
        self.th.join()

    def start(self):
        # Start the proxy in a separate thread
        self.th = threading.Thread(target=self._proxy)
        self.th.start()  




proxy = Proxy(worker_addr = "tcp://*:5560", client_addr = "tcp://*:5559")
proxy.start()
while True:
    try:
        time.sleep(0.1)
    except KeyboardInterrupt:
        print('detected KeyboardInterrupt')
        break
print('stopping')
proxy.stop()

# time.sleep(1)

exit(0)



# Prepare our context and sockets
context = zmq.Context()

frontend = context.socket(zmq.ROUTER)
backend = context.socket(zmq.DEALER)

frontend.bind("tcp://*:5559")
backend.bind("tcp://*:5560")

frontend.setsockopt(zmq.RCVTIMEO, 1000)
frontend.setsockopt(zmq.SNDTIMEO, 1000)
frontend.setsockopt(zmq.LINGER, 0)

backend.setsockopt(zmq.RCVTIMEO, 1000)
backend.setsockopt(zmq.SNDTIMEO, 1000)
backend.setsockopt(zmq.LINGER, 0)


print(dir(backend))
print()
backend.close()
print(backend.closed)
exit(0)

# Initialize poll set
poller = zmq.Poller()
poller.register(frontend, zmq.POLLIN)
poller.register(backend, zmq.POLLIN)

# Switch messages between sockets
while True:
    try:
        print('ola')
        socks = dict(poller.poll(1000))
        print(socks)
        if socks.get(frontend) == zmq.POLLIN:
            message = frontend.recv_multipart()
            backend.send_multipart(message)

        if socks.get(backend) == zmq.POLLIN:
            message = backend.recv_multipart()
            frontend.send_multipart(message)
    except KeyboardInterrupt:
        print('out')
        break

frontend.close()
backend.close()
context.term()