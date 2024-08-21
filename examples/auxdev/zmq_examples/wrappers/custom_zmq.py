import zmq

WORKER_READY = "\x01"

# ZMQ REQ/REP Wrapper
class ZMQR:
    def __init__(self, ctx:zmq.Context, zmq_type:int, timeo:int = 1, identity:str = None, reconnect:bool = True):
        self.timeo = 1000*timeo
        self.ctx = ctx
        self.socket = None
        self.identity = identity
        self.zmq_type = zmq_type
        assert self.zmq_type in [zmq.REQ, zmq.REP], "ZMQR must use a REP or REQ socket"
        self.addr = [] # list of addr
        self.bind_addr = ''
        self.binded = False
        self.reconnect = reconnect

    def set_identity(self, identity:str = None):
        self.identity = identity
        if self.identity: self.socket.setsockopt_string(zmq.IDENTITY, self.identity)

    def _build_socket(self):
        if self.socket: self.close()
        self.socket = self.ctx.socket(self.zmq_type)
        if self.identity: self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.setsockopt(zmq.LINGER, 0)
        
    def bind(self, addr:str = None):
        self.bind_addr = addr if addr else self.bind_addr
        self.binded = True
        self._build_socket()
        self.socket.bind(self.bind_addr)

    def connect(self, addr:str = None):
        assert not self.binded, "trying to connect a socket that was binded"
        if addr not in self.addr and addr: self.addr.append(addr)
        self._build_socket()
        for addr in self.addr:
            self.socket.connect(addr)

    def _reconnect(self):
        if self.binded:
            self.bind()
        else:
            self.connect()

    def close(self):        
        self.socket.close()

    def send(self, msg:str, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_string(msg)
            return 1
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REP:
            if self.reconnect: self._reconnect()
        return 0

    def send_multipart(self, msg, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_multipart(msg)
            return 1
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REP:
            if self.reconnect: self._reconnect()
        return 0        

    def recv(self, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        if (self.socket.poll(timeo) & zmq.POLLIN) != 0:
            reply = self.socket.recv_string()
            return reply
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REQ:
            if self.reconnect: self._reconnect()
        return None

    def recv_multipart(self, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        if (self.socket.poll(timeo) & zmq.POLLIN) != 0:
            reply = self.socket.recv_multipart()
            return reply
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REQ:
            if self.reconnect: self._reconnect()
        return None


class Proxy:
    
    def __init__(self, worker_addr:str, client_addr:str, context:zmq.Context = None):
        
        self.worker_addr = worker_addr
        self.client_addr = client_addr
        
        self.proxy_stop_even = threading.Event()

        self.polltimeo = 100

        self.term_ctx = True
        if context:
            self.term_ctx = False
            self.context = context
        else:
            self.context = zmq.Context.instance()        

    # run the proxy
    def proxy(self):   
        # context = zmq.Context.instance() 

        frontend = self.context.socket(zmq.ROUTER)
        backend = self.context.socket(zmq.DEALER)

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
                print('proxy thread error : ', e)
                
        print('closing proxy')
        frontend.close()
        backend.close()
        if self.term_ctx: 
            self.context.term()
        print('proxy closed')


    def stop(self):
        # closing the sockets will trigger an error inside the thread
        # stopping the proxy
        self.proxy_stop_even.set()
        self.th.join()

    def start(self):
        # Start the proxy in a separate thread
        self.th = threading.Thread(target=self.proxy)
        self.th.start()  







if __name__ == '__main__':
    ctx = zmq.Context()
    s = ZMQReq(ctx)
    s.connect("tcp://localhost:5555")
    s.connect("tcp://localhost:5556")

    s.send("ola")
    msg = s.recv()
    print(msg)
    s.send("ola")
    msg = s.recv()
    print(msg)

    s.close()
    ctx.term()