import zmq
from binascii import hexlify
import binascii

C_CLIENT = "C_CLIENT"
W_WORKER = "W_WORKER"
W_HEARTBEAT = "W_HEARTBEAT"
W_READY = "W_READY"
W_REQUEST = "W_REQUEST"
W_REPLY = "W_REPLY"
W_DISCONNECT = "W_DISCONNECT"

HEARTBEAT_INTERVAL = 5000

# the first field in a message is the address which will not be decoded and encoded
# for the rest we work with strings
def decode(msg:list) -> list:
    return [msg[0]] + [e.decode() for e in msg[1:]]

def encode(msg:list) -> list:
    print(msg)
    return [msg[0]] + [e.encode() for e in msg[1:]]

class Service(object):
    """a single Service"""
    name = None # Service name
    requests = None # List of client requests
    waiting = None # List of waiting workers

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []


class Worker(object):
    """a Worker, idle or active"""
    identity = None # hex Identity of worker
    address = None # Address to route to
    service = None # Owning service, if known
    expiry = None # expires at this point, unless heartbeat

    def __init__(self, identity, address, lifetime):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + 1e-3*lifetime

class ServiceQueue(object):
    """
    Service Queue
    Workers register as providers of a service
    Clients make requests to specific services
    Service Queue routes from clients to the appropriate
        worker and vice verse
    """
    def __init__(self, addr:str):

        self.addr = addr        
        self.ctx = zmq.Context()
        # declare router socket
        self.socket = self.ctx.socket(zmq.ROUTER)
        # set linger to close it without wait
        self.socket.linger = 0
        # build poller
        # self.poller = zmq.Poller()
        # self.poller.register(self.socket, zmq.POLLIN)
        self.socket.bind(addr)

        # poll timeo
        self.timeo = 1000


        # internal variables
        self.services = {}
        self.waiting = []
        self.workers = {}

        
    def recv(self) -> list:
        '''
        recv_multipart and decode
        '''
        if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
            msg = self.socket.recv_multipart()
            return decode(msg)
        return None

    def send(self, msg:list) -> int:
        '''
        msg: list of strings
        '''
        # check if we can send - make poll with a timeout for the operation to write
        if (self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            self.socket.send_multipart(encode(msg))
            return 1
        return 0

    def serve(self):
        print(f'Starting ServiceQueue on {self.addr}')
        while True:
            try:
                msg = self.recv()
                if msg:                                                                            
                    # parse message
                    # if client
                    # msg ~ [sender identity, '', header (=C_CLIENT), service, msg]
                    # if worker
                    # msg ~ [sender identity, '', header (=W_WORKER), service, command, msg]
                    sender = msg.pop(0)
                    empty = msg.pop(0)
                    assert empty == '', "Invalid message delimiter"
                    header = msg.pop(0)
                    # got a msg from a client
                    if (header == C_CLIENT):
                        self.process_client(sender, msg)
                    # got a msg from a worker
                    elif (header == W_WORKER):
                        self.process_worker(sender, msg)
                    else:
                        raise Exception("Invalid message header")
                # delete expired workers
                self.purge_workers()
                # send heartbeats
                # self.send_heartbeats()
            except KeyboardInterrupt:
                print('ServiceQueue KeyboardInterrupt. Exit.')
                break
            except Exception as e:
                print('ServiceQueue error: ', e)
                break # Interrupted
        # close socket
        self.socket.close()
        # destroy context
        self.ctx.destroy()
        print(f'ServiceQueue on {self.addr} terminated')

    def purge_workers(self):
        pass

    def require_service(self, name) -> Service:
        """Locates the service (creates if necessary)."""
        assert name, "Invalid service name"
        service = self.services.get(name)
        if not service:
            service = Service(name)
            self.services[name] = service
        return service

    def require_worker(self, address:bytes):
        """Finds the worker (creates if necessary)."""
        assert address, "Invalid address"
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if not worker:
            worker = Worker(identity, address, HEARTBEAT_EXPIRY)
            self.workers[identity] = worker
            print("Registering new worker: %s", identity)
        return worker


    def send_to_worker(self, worker:Worker, command:str, msg:str = None):
        """Send message to worker.
        If message is provided, sends that message.
        """
        if not msg: msg = ""
        msg = [worker.address, '', W_WORKER, command, msg]
        self.send(msg)

    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        # Queue to broker and service waiting lists
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3*HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service:Service, msg:list):
        """
        Dispatch requests to waiting workers as possible
        service: Service obj
        msg: [requester id, "", msg string with the request]
        """
        assert service, "Invalid service"
        if msg is not None:
            service.requests.append(msg)
        # self.purge_workers()
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            self.send_to_worker(worker, W_REQUEST, msg)

    def process_client(self, sender, msg):
        """
        Process a request coming from a client.
        sender: client id
        msg: list
            msg ~ [service, msg]
        """
        assert len(msg) == 2, "Invalid client message format" 
        service, msg = msg
        # Set reply return address to client sender
        msg = [sender, '', msg]
        self.dispatch(self.require_service(service), msg)

    def process_worker(self, sender:bytes, msg:list):
        '''
        sender: bytes with the worker identity that sent the message
        msg: list 
            msg ~ [service, command, msg]
        '''
        """Process message sent to us by a worker."""
        assert len(msg) == 3, "Invalid worker message format" 

        print('in process_worker ', msg)


        service, command, msg = msg
        # check if worker is in workers
        # worker_ready = sender in self.workers

        worker = self.require_worker(sender)

        # worker sent a heartbeat
        if command == W_HEARTBEAT:
            # assert len(msg) >= 1 # At least, a service name
            # service = msg.pop(0)
            # Not first command in session or Reserved service name
            # if (worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX)):
                self.delete_worker(worker, True)
            # else:
                # Attach worker to service and mark as idle
            worker.service = self.require_service(service)
            self.worker_waiting(worker)

        elif command == W_REPLY:
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0) # ?
                msg = [client, b'', C_CLIENT, worker.service.name] + msg
                self.socket.send_multipart(msg)
                self.worker_waiting(worker)
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_HEARTBEAT == command):
            if (worker_ready):
                worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_DISCONNECT == command):
            self.delete_worker(worker, False)
        else:
            logging.error("E: invalid message:")
            dump(msg)








def main():
    """create and start new broker"""

    addr = "tcp://127.0.0.1:5555"

    q = ServiceQueue(addr)
    q.serve()
    return None

    ctx = zmq.Context()
    # declare router socket
    socket = ctx.socket(zmq.ROUTER)
    # set linger to close it without wait
    socket.linger = 0
    # build poller
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    socket.bind(addr)

    test = {}


    """Main broker work happens here"""
    while True:
        try:
            items = poller.poll(1000)
            if items:
                msg = socket.recv_multipart()
                msg = decode(msg)
                print('got msg: ', msg)

                client = msg[0]
                # send back a reply
                print('reply')
                test.update({client:'ola'})

                msg = ['reply from queue']

                print(test)
                # client = "sfjdslivhbsd"
                msg = [client, '', C_CLIENT, "service"] + msg
                msg = encode(msg)
                socket.send_multipart(msg)


        except KeyboardInterrupt:
            break # Interrupted


if __name__ == '__main__':
    main()
