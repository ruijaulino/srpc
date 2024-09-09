import zmq
import threading
from random import randint
import time
import random
import string
import datetime as dt
from collections import OrderedDict

# default message to use in Queue to signal the worker is ready
COMM_ALIVE = "\x01"
COMM_HEARTBEAT_INTERVAL = 5
ENCODING = 'utf-8' #'Windows-1252'

# for service queue
COMM_HEADER_WORKER = "W"
COMM_HEADER_CLIENT = "C"
COMM_TYPE_HEARTBEAT = "H"
COMM_TYPE_REP = "REP"
COMM_TYPE_REQ = "REQ"
COMM_MSG_HEARTBEAT = ""

def generate_random_string(n):
    # Define the character set: lowercase, uppercase letters, digits
    characters = string.ascii_letters + string.digits
    # Generate a random string
    random_string = ''.join(random.choice(characters) for _ in range(n))
    return random_string

def create_identity():
    return "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))  


def ts():
    return dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Generic, asynch and stoppable ZMQ REQ/REP socket in a wrapper
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
            enc_msg = msg.encode()
        elif isinstance(msg, list):
            enc_msg = []
            for e in msg: 
                if isinstance(e, str):
                    e = e.encode()
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
            dec_msg = msg.decode()
        elif isinstance(msg, list):
            dec_msg = []
            for e in msg: 
                if isinstance(e, bytes):
                    e = e.decode()
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
        timeo = int(1000*timeo) if timeo else self.timeo
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
        timeo = int(1000*timeo) if timeo else self.timeo
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_multipart(self._encode_msg(msg))
            return 1
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type == zmq.REP:
            if self.reconnect: self._reconnect()
        return 0        

    def recv(self, timeo:int = None):
        timeo = int(1000*timeo) if timeo else self.timeo
        if (self.socket.poll(timeo) & zmq.POLLIN) != 0:
            msg = self.socket.recv()
            msg = self._decode_msg(msg)
            return msg
        # reset the state to be able to send again if zmq.REQ
        if self.zmq_type in [zmq.REQ, zmq.DEALER]:
            if self.reconnect: self._reconnect()
        return None

    def recv_multipart(self, timeo:int = None):
        timeo = int(1000*timeo) if timeo else self.timeo
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
        """Look for & kill workers that are not sending heartbeats"""
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
                    address = frames[0]
                    workers.ready(ReliableWorker(address))
                    # Validate control message, or return reply to client
                    msg = frames[1:]
                    if len(msg) == 1:
                        if msg[0] == COMM_ALIVE.encode():                            
                            # reply imediately to the heartbeat
                            msg = [address, COMM_ALIVE.encode()]
                            backend.send_multipart(msg)
                        else:
                            print(f'Worker {address} sent msg with unknown format')
                    else:
                        # send worker reply to the client via frontend
                        frontend.send_multipart(msg)
                # Handle clients requests in the frontend
                if socks.get(frontend) == zmq.POLLIN:
                    frames = frontend.recv_multipart()
                    frames.insert(0, workers.next())
                    backend.send_multipart(frames)
                workers.purge()
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

# implement a worker for the paranoid pirate pattern
# uses ping-pong between worker and queue
# the workers pings the queue and it should answer
# if it does not answer within the heartbeat time, then the worker assumes the queue is dead
# and starts a new connection
# when the queue get a message from the worker puts that worker to be used
class ZMQReliableQueueWorker(ZMQR):
    # should always receive and send in multipart because the queue need to handle the envelope
    def __init__(self, ctx:zmq.Context):
        '''
        '''
        ZMQR.__init__(
                        self, 
                        ctx = ctx, 
                        zmq_type = zmq.DEALER, 
                        timeo = COMM_HEARTBEAT_INTERVAL/10, 
                        identity = create_identity(), 
                        reconnect = False
                        )                
        self.ping_t = None        
        self.pong_at = None
        self.queue_alive = False
    
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
        self.ping_t = time.time()#  + COMM_HEARTBEAT_INTERVAL
        self.pong_t = time.time()
        self.queue_alive = False
        self.send(COMM_ALIVE)

    def recv_work(self):
        '''
        receive work and handle heartbeats
        should receive a multipart message
        like ['client addr/identity','','request']
        '''
        # is a message is not received in this time it means that the queue is dead
        # keeps reconnecting
        msg = self.recv_multipart()
        clientid = None
        if msg:    
            # if we get something means that the queue is up
            self.queue_alive = True
            self.pong_t = time.time()
            # received a message with work
            if len(msg) == 3:
                clientid, msg = msg[0], msg[2]
            elif len(msg) == 1 and msg[0] == COMM_ALIVE:                
                msg, clientid = None, None
            else:
                # received a pong/other message with a unknown format
                print(f"Unknown message format from queue")
                msg, clientid = None, None
        # queue is not responding condition
        if not self.queue_alive and (time.time() - self.pong_t > COMM_HEARTBEAT_INTERVAL):
            print(f"Queue not responding to worker {self.identity}")
            self.connect()
        else: 
            # signal to the queue that worker is alive by pinging it
            if time.time() > self.ping_t + COMM_HEARTBEAT_INTERVAL:
                self.ping_t = time.time()
                self.queue_alive = False
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
        sub_socket.setsockopt(zmq.LINGER, 0)

        # comm with exterior is made with a xpub
        xpub_socket = self.ctx.socket(zmq.XPUB) 
        xpub_socket.bind(self.addr)        
        xpub_socket.setsockopt(zmq.LINGER, 0)
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
        self.socket.setsockopt(zmq.LINGER, 0)
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
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.CONFLATE, self.conflate) 
        self.socket.connect(self.addr)

    def unsubscribe(self, topic:str):
        self.socket.unsubscribe(topic.encode())

    def subscribe(self, topic:str = ''):
        self.socket.subscribe(topic.encode())
    
    def recv(self):
        if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
            topic, msg = self.socket.recv_multipart()
            return topic.decode(), msg.decode()
        return None, None
    
    def close(self):
        self.socket.close()



# proxy with last value caching
def ZMQProxy(pub_addr:str, sub_addr:str, topics_no_cache = ['trigger'], stop_event:threading.Event = None):


    print(f'[{ts()}] Starting Proxy from {sub_addr} to {pub_addr}')

    stop_event = stop_event if stop_event else threading.Event()

    ctx = zmq.Context.instance()

    xsub_socket = ctx.socket(zmq.XSUB) 
    xsub_socket.bind(sub_addr)
    xsub_socket.setsockopt(zmq.LINGER, 0)

    xpub_socket = ctx.socket(zmq.XPUB) 
    xpub_socket.bind(pub_addr)
    xpub_socket.setsockopt(zmq.LINGER, 0)
    # this flag is needed otherwise the xpub will not receive repeated topics sub request
    # and we cannot reroute the last message
    xpub_socket.setsockopt(zmq.XPUB_VERBOSE, True) 

    # poller for sockets
    poller = zmq.Poller()
    poller.register(xsub_socket, zmq.POLLIN)
    poller.register(xpub_socket, zmq.POLLIN)   

    # cache
    cache = {}
    # make the xsub receive all messages
    xsub_socket.send_multipart([b'\x01'])

    while not stop_event.isSet():            
        try:
            # poll for events
            events = dict(poller.poll(1000))
            # for any new topic data, cache it and then forward
            if xsub_socket in events:
                # get msg
                tmp = xsub_socket.recv_multipart()
                # update cache 
                topic, msg = tmp                
                dec_topic = topic.decode()
                c = True
                for t in topics_no_cache:
                    if dec_topic.startswith(t):
                        c = False
                        break
                if c:
                    cache[dec_topic] = msg.decode()
                # publish back
                xpub_socket.send_multipart(tmp)

            # handle subscriptions
            # When we get a new subscription we pull data from the cache:
            if xpub_socket in events:                
                msg = xpub_socket.recv()
                # Event is one byte 0=unsub or 1=sub, followed by topic            
                # print(cache)
                if msg[0] == 1:
                    st = msg[1:].decode()
                    # need to run all that startswith
                    for k,v in cache.items():
                        if k.startswith(st):
                            xpub_socket.send_multipart([k.encode(), v.encode()])
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f'[{ts()}] Error in proxy: ', e)
            break

    print(f'[{ts()}] Terminating Proxy')
    
    # close sockets
    xsub_socket.close()
    xpub_socket.close()

    # terminate context
    ctx.destroy()


# SOCKETS FOR PROXY

class ZMQPub:
    def __init__(self, ctx:zmq.Context = None, timeo:int = 1):
        self.ctx = ctx
        self.timeo = 1000*timeo if timeo else 1000        
        self.addr = None
        self.socket = None

    def connect(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.addr)
        # time.sleep(1) # make sure all connections have time to be established
    
    def publish(self, topic:str, msg:str):        
        if (self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:            
            self.socket.send_multipart([topic.encode(), msg.encode()])
            
    def close(self):
        self.socket.close()

class ZMQSub:
    def __init__(self, ctx:zmq.Context = None, last_msg_only:bool = False, timeo:int = 1):
        self.ctx = ctx
        self.conflate = 1 if last_msg_only else 0
        self.timeo = 1000*timeo if timeo else 1000        
        self.addr = None
        self.socket = None

    def connect(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.CONFLATE, self.conflate) 
        self.socket.connect(self.addr)
        # time.sleep(1) # make sure all connections have time to be established

    def unsubscribe(self, topic:str):
        self.socket.unsubscribe(topic.encode())

    def subscribe(self, topic:str = ''):
        self.socket.subscribe(topic.encode())
    
    def recv(self):
        if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
            topic, msg = self.socket.recv_multipart()
            return topic.decode(), msg.decode()
        return None, None
    
    def close(self):
        self.socket.close()

# SERVICES BROKER
class ZMQServiceBrokerService:
    """a single Service"""
    def __init__(self, name:str, msg_timeo:int = 20):
        self.name = name
        self.msg_timeo = msg_timeo
        self.requests = [] # requests to the service
        self.workers_address = [] # OrderedDict() # workers queue
        self.workers_expiry = []    

    def _pop_worker(self, idx:int):
        self.workers_expiry.pop(idx)        
        tmp = self.workers_address.pop(idx)
        return tmp

    def add_request(self, msg):
        self.requests.append({'msg':msg, 'expiry':time.time()+self.msg_timeo})

    def add_worker(self, address):
        # remove if exists
        if address in self.workers_address:
            _ = self._pop_worker(self.workers_address.index(address))        
        self.workers_address.append(address)
        self.workers_expiry.append(time.time() + COMM_HEARTBEAT_INTERVAL*2)
        
    def next_worker(self):
        '''
        returns the next worker address
        '''
        return self._pop_worker(0)

    def next_request(self):
        return self.requests.pop(0).get('msg')

    
    def purge_requests(self):
        new_requests = []
        for e in self.requests:
            if time.time() < e.get('expiry'): 
                new_requests.append(e)
            else:
                print(f"[{ts()}] Request to service {self.name} expired | {e.get('msg')}")
        self.requests = new_requests        

    def purge_workers(self):
        """Look for & kill workers that are not sending heartbeats"""
        t = time.time()
        expired_idx = []
        for i, e in enumerate(self.workers_expiry):
            if t > e:  # Worker expired
                expired_idx.append(i)
        for i in expired_idx:
            print(f"[{ts()}] Idle worker {self.workers_address[i]} for service {self.name} expired" )
            _ = self._pop_worker(i)

    def purge(self):
        self.purge_requests()
        self.purge_workers()

    def has_workers(self):
        return len(self.workers_address) > 0

    def has_requests(self):
        # purge requests that are not being served
        self.purge_requests()
        return len(self.requests) > 0

    def has_workers_and_requests(self):
        return self.has_workers() and self.has_requests()

class ZMQServiceBroker:
    """
    Service Queue
    Workers register as providers of a service
    Clients make requests to specific services
    Service Queue routes from clients to the appropriate
        worker and vice verse
    """
    def __init__(self, addr:str, timeo:int = 1000, stop_event:threading.Event = None):
        '''
        addr: address where to bind and receive comm from workers and clients
        timeo: timeout for the poller
        '''
        self.addr = addr        
        self.ctx = zmq.Context()
        # declare router socket
        self.socket = self.ctx.socket(zmq.ROUTER)
        # set linger to close it without wait
        self.socket.linger = 0
        # bind the socket
        self.socket.bind(addr)
        # poll timeo
        self.timeo = timeo        
        # external stop signal
        self.stop_event = stop_event if stop_event else threading.Event()

        self.services = {}


    # send/receive methods
    def _decode(self, msg:list) -> list:
        return [e.decode() for e in msg]

    def _encode(self, msg:list) -> list:
        return [e.encode() for e in msg]

    def recv(self) -> list:
        '''
        recv_multipart and decode
        '''
        if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
            msg = self.socket.recv_multipart()
            return self._decode(msg)
        return None

    def send(self, msg:list) -> int:
        '''
        msg: list of strings
        '''
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_multipart(self._encode(msg))
            return 1
        return 0

    def require_service(self, service:str):
        if not self.has_service(service):        
            print(f"[{ts()}] Registering service {service}")    
            self.services[service] = ZMQServiceBrokerService(service)
        return self.services[service]

    def has_service(self, service:str):
        return service in self.services

    def purge(self):
        to_del = []
        for name, service in self.services.items():
            service.purge()
            if not service.has_workers(): to_del.append(name)
        # purge the services without workers left if we are not allowed
        # to create services on client requests 
        # if not self.create_service_on_client:
        # for name in to_del: 
        #     print(f"Unegistering service {name} because there are no workers left")    
        #     self.services.pop(name)

    def dispatch(self, service:ZMQServiceBrokerService, msg:str = None):
        """Dispatch requests to waiting workers as possible"""
        # Queue message
        if msg is not None:
            service.add_request(msg)                    
        # dispatch all message while there are requests and workers
        while service.has_workers_and_requests():            
            msg = service.next_request()
            address = service.next_worker()
            self.send([address, COMM_TYPE_REQ]+msg)

    def serve(self):
        print(f'[{ts()}] Starting ZMQServiceBroker on {self.addr}')
        while not self.stop_event.isSet():            
            try:
                msg = self.recv()
                if msg:  
                    # parse message
                    # if client
                    # msg ~ [sender identity, '', header, service, msg]
                    # if worker
                    # msg ~ [sender identity, '', header, comm_type, service, <client id>, msg]
                    
                    # parse sender address 
                    sender = msg.pop(0)
                    # there is an empty string then (dealer socket make the messages to match this)
                    empty = msg.pop(0)
                    # parse header info (distinguish between client and worker comm)
                    header = msg.pop(0)
                    
                    # got a msg from a client
                    if (header == COMM_HEADER_CLIENT):
                        # at this point client messages must contain
                        # the target service and the payload
                        if len(msg) == 2:
                            service, msg = msg
                            # add sender to msg
                            msg = [sender, msg]
                            # dispatch to service
                            self.dispatch(self.require_service(service), msg)

                            # if not self.create_service_on_client and not self.has_service(service):
                            #     self.send([sender, '', 'ERROR'])
                            # else:
                            #     self.dispatch(self.require_service(service), msg)                    
                        else:
                            print(f"[{ts()}] Invalid message from client")

                    # got a msg from a worker                    
                    elif (header == COMM_HEADER_WORKER):
                        # at this point, worker messages must contain
                        # info about the type of communication that they are
                        # making and info about their own service
                        #
                        comm_type = msg.pop(0)
                        service = msg.pop(0)

                        # reset the service because we just received a new message 
                        # from it, it is ready for new payloads
                        service = self.require_service(service)
                        service.add_worker(sender)

                        # reply to heartbeat
                        if comm_type == COMM_TYPE_HEARTBEAT:
                            msg = [sender, COMM_TYPE_HEARTBEAT, COMM_MSG_HEARTBEAT]
                            self.send(msg)

                        # send back to the client                            
                        elif comm_type == COMM_TYPE_REP:
                            # here, now message should be like [client id, msg]
                            # to route the reply to the appropriate client
                            if len(msg) == 2:
                                # add the empty string to match between dealer and req sockets
                                self.send([msg[0], '', msg[1]])
                            else:
                                print(f"[{ts()}] Wrong format reply message from worker")                            
                        else:
                            print(f'[{ts()}] Unknown message from worker')
                        # dispatch worker to worker on pending payloads
                        self.dispatch(service)                           
                    else:
                        raise Exception("Invalid message header")
                # delete expired workers
                self.purge()

            except KeyboardInterrupt:
                print(f'[{ts()}] ZMQServiceBroker KeyboardInterrupt. ')
                break
            except Exception as e:
                print(f'[{ts()}] ZMQServiceBroker error: ', e)
                break # Interrupted
        
        print(f'[{ts()}] terminating ZMQServiceBroker...')
        # close socket
        self.socket.close()
        # destroy context
        self.ctx.destroy()
        print(f'[{ts()}] ZMQServiceBroker on {self.addr} terminated')


class ZMQServiceBrokerWorker(ZMQR):
    # should always receive and send in multipart because the queue need to handle the envelope
    def __init__(self, ctx:zmq.Context, service:str):
        '''
        '''
        ZMQR.__init__(
                        self, 
                        ctx = ctx, 
                        zmq_type = zmq.DEALER, 
                        timeo = COMM_HEARTBEAT_INTERVAL/10, 
                        identity = create_identity(), 
                        reconnect = False
                        )                
        self.service = service
        self.ping_t = None        
        self.pong_at = None
        self.queue_alive = False
    
    def connect(self, addr:str = None):
        '''
        connect the socket
        send message that is ready
        '''
        # create new identity
        self.set_identity(create_identity())
        # use the private method
        self._connect(addr)
        print(f"[{ts()}] Service {self.service} worker {self.identity} ready")
        self.ping_t = time.time()
        self.pong_t = time.time()
        self.queue_alive = False
        self.send_heartbeat()

    def send_heartbeat(self):
        self.send_multipart(['',COMM_HEADER_WORKER, COMM_TYPE_HEARTBEAT, self.service, COMM_MSG_HEARTBEAT])

    def recv_work(self):
        '''
        receive work and handle heartbeats
        should receive a multipart message
        like ['client addr/identity','','request']
        '''
        # is a message is not received in this time it means that the queue is dead
        # keeps reconnecting
        msg = self.recv_multipart()
        clientid = None
        if msg:
            comm_type = msg.pop(0)
            # if we get something means that the queue is up
            self.queue_alive = True
            self.pong_t = time.time()
            # received a message with work
            if comm_type == COMM_TYPE_REQ:
                clientid, msg = msg[0], msg[1]
            
            elif comm_type == COMM_TYPE_HEARTBEAT:
                msg = msg.pop(0)
                if msg != COMM_MSG_HEARTBEAT:
                    print(f"[{ts()}] Unknown heartbeat message from queue")                
                msg, clientid = None, None
            else:
                # received a pong/other message with a unknown format
                print(f"[{ts()}] Unknown message format from queue")
                msg, clientid = None, None

        # queue is not responding condition
        if not self.queue_alive and (time.time() - self.pong_t > COMM_HEARTBEAT_INTERVAL):
            print(f"[{ts()}] Queue not responding to worker {self.identity}")
            self.connect()
        else: 
            # signal to the queue that worker is alive by pinging it
            if time.time() > self.ping_t + COMM_HEARTBEAT_INTERVAL:
                self.ping_t = time.time()
                self.queue_alive = False
                self.send_heartbeat()
        return clientid, msg

    def send_work(self, clientid:str, msg:str):
        return self.send_multipart(['',COMM_HEADER_WORKER, COMM_TYPE_REP, self.service, clientid, msg])


class ZMQServiceBrokerClient(ZMQR):
    # should always receive and send in multipart because the queue need to handle the envelope
    def __init__(self, ctx:zmq.Context):
        '''
        '''
        ZMQR.__init__(
                        self, 
                        ctx = ctx, 
                        zmq_type = zmq.REQ, 
                        timeo = COMM_HEARTBEAT_INTERVAL/10, 
                        identity = create_identity(), 
                        reconnect = True
                        )                

    def connect(self, addr:str = None):
        '''
        connect the socket
        send message that is ready
        '''
        # create new identity
        self.set_identity(create_identity())
        # use the private method
        self._connect(addr)
        print(f"[{ts()}] Client to service queue {self.identity} ready")
        
    def req(self, service:str, msg:str, timeo:int = None):
        return self.send_multipart([COMM_HEADER_CLIENT, service, msg], timeo = timeo)

    def rep(self, timeo = None):
        return self.recv(timeo = timeo) 

    def request(self, service:str, msg:str):
        '''
        make a request to the queue
        wait for reply
        '''
        status = self.send_multipart([COMM_HEADER_CLIENT, service, msg])
        if status == 1:
            # wait for reply
            rep = self.recv()            
            return rep
        else:
            print('Could not send request to service queue')
            return None




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