import zmq
import threading
from random import randint
import time
import random
import string
from collections import OrderedDict

# default message to use in Queue to signal the worker is ready
COMM_ALIVE = "\x01"
COMM_HEARTBEAT_INTERVAL = 3
# COMM_TOPIC_ALL = '0all'
ENCODING = 'utf-8' #'Windows-1252'

def generate_random_string(n):
    # Define the character set: lowercase, uppercase letters, digits
    characters = string.ascii_letters + string.digits
    # Generate a random string
    random_string = ''.join(random.choice(characters) for _ in range(n))
    return random_string

def create_identity():
    return "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))  


# Generica, asynch and stoppable ZMQ REQ/REP socket in a wrapper
class ZMQR:
    def __init__(self, ctx:zmq.Context, zmq_type:int, timeo:int = 1, identity:str = None, reconnect:bool = True):
        self.timeo = 1000*timeo
        self.ctx = ctx
        self.socket = None
        self.identity = identity if identity else create_identity()
        self.zmq_type = zmq_type
        assert self.zmq_type in [zmq.REQ, zmq.REP, zmq.DEALER], "ZMQR must use a REP or REQ socket"
        self.addr = [] # list of addr
        self.bind_addr = ''
        self.binded = False
        self.reconnect = reconnect

    def set_identity(self, identity:str = None):
        self.identity = identity
        if self.socket and self.identity: self.socket.setsockopt_string(zmq.IDENTITY, self.identity)

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

    def _connect(self, addr:str = None):
        assert not self.binded, "trying to connect a socket that was binded"
        if addr not in self.addr and addr: self.addr.append(addr)
        self._build_socket()
        for addr in self.addr:
            self.socket.connect(addr)

    # override if necessary
    def connect(self, addr:str = None):
        self._connect(addr)

    def _encode_msg(self, msg):
        '''
        msg: str or list of strings
        '''

        enc_msg = "\x01".encode()
        if isinstance(msg, str):
            #try:
            enc_msg = msg.encode()
            #except:
            #    enc_msg = msg

        elif isinstance(msg, list):
            enc_msg = []
            for e in msg: 
                if isinstance(e, str):
                    #try:
                    e = e.encode()
                    #except:
                    #    pass
                enc_msg.append(e)
        else:
            enc_msg = msg
        return enc_msg   

    def _decode_msg(self, msg):
        '''
        msg: str or list of strings
        Note: when we are controling the envelope, zmq can attribute
        identities to sockets that are not decodable, that is the main reason
        we do the try/except here. other solution to avoid this would be to force
        all clients to have an identity (random one)
        '''
        dec_msg = ""
        if isinstance(msg, bytes):
            #try:
            dec_msg = msg.decode()
            #except:
            #    dec_msg = msg
        elif isinstance(msg, list):
            dec_msg = []
            for e in msg: 
                if isinstance(e, bytes):
                    #try:
                    e = e.decode()
                    #except:
                    #    pass
                dec_msg.append(e)
        else:
            dec_msg = msg
        return dec_msg   

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
            self.socket.send(self._encode_msg(msg))
            return 1
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REP:
            if self.reconnect: self._reconnect()
        return 0

    def send_multipart(self, msg, timeo:int = None):
        '''
        msg: list of strings
        '''
        timeo = 1000*timeo if timeo else self.timeo
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_multipart(self._encode_msg(msg))
            return 1
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REP:
            if self.reconnect: self._reconnect()
        return 0        

    def recv(self, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        if (self.socket.poll(timeo) & zmq.POLLIN) != 0:
            msg = self.socket.recv()
            msg = self._decode_msg(msg)
            return msg
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type in [zmq.REQ, zmq.DEALER]:
            if self.reconnect: self._reconnect()
        return None

    def recv_multipart(self, timeo:int = None):
        timeo = 1000*timeo if timeo else self.timeo
        if (self.socket.poll(timeo) & zmq.POLLIN) != 0:
            msg = self.socket.recv_multipart()
            msg = self._decode_msg(msg)
            return msg
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type in [zmq.REQ, zmq.DEALER]:
            if self.reconnect: self._reconnect()
        return None

# --------------------------------------------------
# implements a queue for the simple pirate pattern
# - ROUTER to ROUTER
# - when worker connect they send a predefined message stating that they are ready to perform tasks
# - assumes that the workers dont die
class ZMQSimpleQueue:
    def __init__(self, ctx:zmq.Context, client_addr:str, worker_addr:str, poll_timeo:int = 1, standalone:bool = False):
        '''
        ctx: zmq.Context 
            context

        standalone: bool 
            means that the Queue is supposed to be used in a single program
            of it own like
                q = ZMQQueue(ctx, "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556")
                q.start()
                exit(0)
        '''
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
                    # Use worker address for routing
                    msg = backend.recv_multipart()
                    address = msg[0]
                    workers.append(address)
                    # Everything after the second (delimiter) frame is reply
                    reply = msg[2:]
                    # Forward message to client if it's not a READY message
                    if reply[0] != COMM_ALIVE.encode(ENCODING):
                        frontend.send_multipart(reply)
                    else:
                        print(f'Worker {address.decode()} is ready')
                if socks.get(frontend) == zmq.POLLIN:
                    #  Get client request, route to first available worker
                    msg = frontend.recv_multipart()
                    request = [workers.pop(0), ''.encode(ENCODING)] + msg
                    backend.send_multipart(request)

            except Exception as e:
                print('ZMQQueue thread error : ', e)
                if self.standalone: 
                    break

        frontend.close()
        backend.close()

    def stop(self):
        '''
        stops the queue
        '''
        # closing the sockets will trigger an error inside the thread
        # stopping the proxy        
        self._stopevent.set()
        if self.th: self.th.join()
        
    def start(self):
        '''
        starts the queue
        '''
        if self.standalone:
            self.queue()
        else:
            # Start the proxy in a separate thread
            self.th = threading.Thread(target=self.queue)
            self.th.start()              


# implement a worker for the simple pirate pattern
# when connects, send a message informing that it is ready to perform tasks
# should be used with recv_multipart and send_multipart to handle the envelope
# for example
#   when receiving a 
class ZMQSimpleQueueWorker(ZMQR):
    # should always receive and send in multipart because the queue need to handle the envelope
    def __init__(self, ctx:zmq.Context, timeo:int = 1, identity:str = None):
        # this socket should not reconnect
        ZMQR.__init__(self, ctx = ctx, zmq_type = zmq.REQ, timeo = timeo, identity = identity, reconnect = False)

    def connect(self, addr):
        '''
        connect the socket
        send message that is ready
        '''
        # use the private method
        self._connect(addr)
        print("%s worker ready" % self.identity)
        self.send(COMM_ALIVE)

    def recv_work(self):
        '''
        receive work
        should receive a multipart message
        like ['client addr/identity','','request']
        '''
        msg = self.recv_multipart()
        clientid = None
        if msg:
            if len(msg) == 3:
                clientid, msg = msg[0], msg[2]
            else:
                msg = None
                print('recv invalid message format')
        return clientid, msg

    def send_work(self, clientid:str, msg:str):
        self.send_multipart([clientid,'',msg])



# --------------------------------------------------
# implements a queue for the paranoid pirate pattern
# - ROUTER to ROUTER
# - exchange heartbeats

class ReliableWorker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + COMM_HEARTBEAT_INTERVAL*2

class ReliableWorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address, worker in self.queue.items():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print("Idle worker expired: %s" % address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

    def __len__(self):
        return len(self.queue)

class ZMQReliableQueue:
    def __init__(self, ctx:zmq.Context, client_addr:str, worker_addr:str, poll_timeo:int = 1, standalone:bool = False):
        '''
        ctx: zmq.Context 
            context

        standalone: bool 
            means that the Queue is supposed to be used in a single program
            of it own like
                q = ZMQQueue(ctx, "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556")
                q.start()
                exit(0)
        '''
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

        workers = ReliableWorkerQueue()

        heartbeat_at = time.time() + COMM_HEARTBEAT_INTERVAL

        while self._condition():
            try:
                if len(workers) > 0:
                    poller = poll_both
                else:
                    poller = poll_workers
                socks = dict(poller.poll(COMM_HEARTBEAT_INTERVAL * 1000))

                # Handle worker activity on backend
                if socks.get(backend) == zmq.POLLIN:
                    # Use worker address for LRU routing
                    frames = backend.recv_multipart()
                    #print('backend received: ', frames)
                    address = frames[0]
                    workers.ready(ReliableWorker(address))

                    # Validate control message, or return reply to client
                    msg = frames[1:]
                    if len(msg) == 1:
                        if msg[0] != COMM_ALIVE.encode():
                            print(f'Worker {address} sent msg with wrong format')

                    else:
                        # print('backend send to frontend: ', msg)
                        frontend.send_multipart(msg)

                if socks.get(frontend) == zmq.POLLIN:
                    frames = frontend.recv_multipart()
                    #print('frontend received: ', frames)
                    frames.insert(0, workers.next())
                    #print('frontend send to backend: ', frames)
                    backend.send_multipart(frames)

                # Send heartbeats to idle workers if it's time
                if time.time() >= heartbeat_at:
                    for worker in workers.queue:
                        msg = [worker, COMM_ALIVE.encode()]
                        backend.send_multipart(msg)
                    heartbeat_at = time.time() + COMM_HEARTBEAT_INTERVAL

                workers.purge()

            except Exception as e:
                print('ZMQQueue thread error : ', e)
                if self.standalone: 
                    break            
    def stop(self):
        '''
        stops the queue
        '''
        # closing the sockets will trigger an error inside the thread
        # stopping the proxy        
        self._stopevent.set()
        if self.th: self.th.join()
        
    def start(self):
        '''
        starts the queue
        '''
        if self.standalone:
            self.queue()
        else:
            # Start the proxy in a separate thread
            self.th = threading.Thread(target=self.queue)
            self.th.start()              

# implement a worker for the paranoid pirate pattern
# keeps exchanging heartbeats
class ZMQReliableQueueWorker(ZMQR):
    # should always receive and send in multipart because the queue need to handle the envelope
    def __init__(self, ctx:zmq.Context):
        '''
        '''
        ZMQR.__init__(self, ctx = ctx, zmq_type = zmq.DEALER, timeo = 2*COMM_HEARTBEAT_INTERVAL, identity = create_identity(), reconnect = True)
        self.heartbeat_at = time.time() + COMM_HEARTBEAT_INTERVAL
        self.queue_dead = False

      

    def connect(self, addr:str = None):
        '''
        connect the socket
        send message that is ready
        '''
        # create new identity
        self.set_identity(create_identity())
        # use the private method
        self._connect(addr)
        print("%s worker ready" % self.identity)
        self.send(COMM_ALIVE)

    def recv_work(self):
        '''
        receive work and handle heartbeats
        should receive a multipart message
        like ['client addr/identity','','request']
        '''
        # is a message is not received in this time it means that the queue is dead
        # keeps reconnecting
        msg = self.recv_multipart(timeo = COMM_HEARTBEAT_INTERVAL*2)
        clientid = None
        if msg:
            if len(msg) == 3:
                clientid, msg = msg[0], msg[2]
            elif len(msg) == 1 and msg[0] == COMM_ALIVE:
                msg = None
        else:
            self.queue_dead = True
        # signal to the queue that worker is alive
        if time.time() > self.heartbeat_at:
            self.heartbeat_at = time.time() + COMM_HEARTBEAT_INTERVAL
            self.send(COMM_ALIVE)
        return clientid, msg

    def send_work(self, clientid:str, msg:str):
        return self.send_multipart([clientid,'',msg])

# --------------------------------------------
# pub socket with last value caching
# if we are doing LVC, we create an internal socket to pub inside the thread
# otherwise we bind directly this pub socket to the exterior
# maybe could have just used a queue but in that case we would 
class ZMQP:
    def __init__(self, ctx:zmq.Context = None, lvc:bool = True, timeo:int = 1):
        self.ctx = ctx
        self.lvc = lvc        
        self.addr = None
        self.th = None
        self._stopevent = threading.Event()
        self._lock = threading.Lock()
        self.timeo = 1000*timeo if timeo else 1000
        self.socket = None
        self._inproc_addr = "inproc://"+generate_random_string(8)

    def _lvc(self):
        # detect new subscribers
        # sub to the internal socket
        # xpub to outside

        # cache
        cache = {}

        # sub to internal socket
        sub_socket = self.ctx.socket(zmq.SUB)
        sub_socket.connect(self._inproc_addr)
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"")
        
        # comm with exterior is made with a xpub
        xpub_socket = self.ctx.socket(zmq.XPUB) 
        xpub_socket.bind(self.addr)        
        # this flag is needed otherwise the xpub will not receive repeated topics sub request
        # and we cannot reroute the last message
        xpub_socket.setsockopt(zmq.XPUB_VERBOSE, True) 

        # poller for sockets
        poller = zmq.Poller()
        poller.register(sub_socket, zmq.POLLIN)
        poller.register(xpub_socket, zmq.POLLIN)        

        print('ZMQP lvc thread start')
        while not self._stopevent.isSet():            
            
            events = dict(poller.poll(1000))
            # for any new topic data, cache it and then forward
            if sub_socket in events:
                tmp = sub_socket.recv_multipart()
                topic, msg = tmp                
                cache[topic.decode()] = msg.decode()
                xpub_socket.send_multipart(tmp)

            # handle subscriptions
            # When we get a new subscription we pull data from the cache:
            if xpub_socket in events:
                msg = xpub_socket.recv()
                # Event is one byte 0=unsub or 1=sub, followed by topic
                
                if msg[0] == 1:
                    st = msg[1:].decode()
                    # need to run all that startswith
                    for k,v in cache.items():
                        if k.startswith(st):
                            xpub_socket.send_multipart([k.encode(), v.encode()])
        
        print('ZMQP lvc thread closed')
        sub_socket.close()


    def bind(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.PUB)
        if self.lvc:
            # sockets binds to a random address inproc
            self.socket.bind(self._inproc_addr)
            self.th = threading.Thread(target = self._lvc)
            self.th.start()
        else:
            # comm with exterior is made with a pub
            self.socket.bind(self.addr)

    def publish(self, topic:str, msg:str):        
        if (self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:            
            self.socket.send_multipart([topic.encode(), msg.encode()])
                
    def close(self):
        if self.lvc:
            self._stopevent.set()
            self.th.join()
        self.socket.close()

# ---------------------------------
# SUB socket
class ZMQS:
    def __init__(self, ctx:zmq.Context = None, last_msg_only:bool = False, timeo:int = 1):
        self.ctx = ctx
        self.conflate = 1 if last_msg_only else 0
        self.timeo = 1000*timeo if timeo else 1000        
        self.addr = None
        self.socket = None

    def connect(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.CONFLATE, self.conflate) 
        self.socket.connect(self.addr)

    def unsubscribe(self, topic:str):
        self.socket.unsubscribe(topic.encode())

    def subscribe(self, topic:str = ''):
        self.socket.subscribe(''.encode())
    
    def recv(self):
        if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
            topic, msg = self.socket.recv_multipart()
            return topic.decode(), msg.decode()
        return None, None
    
    def close(self):
        self.socket.close()

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