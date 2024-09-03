from random import randint
import time
from custom_zmq import ZMQR, ZMQReliableQueueWorker, COMM_HEARTBEAT_INTERVAL, create_identity
from custom_zmq import COMM_HEADER_WORKER, COMM_HEADER_CLIENT, COMM_TYPE_HEARTBEAT, COMM_TYPE_REP, COMM_TYPE_REQ, COMM_MSG_HEARTBEAT
import zmq
import json

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

class Service:
    """a single Service"""
    def __init__(self, name):
        self.name = name
        self.requests = [] # requests to the service
        self.waiting = [] # workers waiting

class Worker:
    """a Worker, idle or active"""
    def __init__(self, address):
        self.address = address
        self.service = None 
        self.expiry = time.time() + COMM_HEARTBEAT_INTERVAL*2

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
        
        self.workers = {}
        self.services = {}
        self.waiting = []

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

    # ----------------------------


    def dispatch(self, service, msg):
        """Dispatch requests to waiting workers as possible"""
        
        # Queue message
        if msg is not None:
            service.requests.append(msg)
        # self.purge_workers() ??????
        
        # dispatch all message while there are requests and workers
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            # send to worker
            print('send to worker ', [COMM_TYPE_REQ]+msg)
            self.send([COMM_TYPE_REQ]+msg)

    def require_service(self, name):
        """Locates the service (creates if necessary)."""
        assert (name is not None)
        service = self.services.get(name)
        if not service:
            print(f"Registering service {name}")
            service = Service(name)
            self.services[name] = service
        return service

    def require_worker(self, address:str):
        """
        Finds the worker (creates if necessary).
        """
        worker = self.workers.get(address)
        if not worker:
            worker = Worker(address)
            self.workers[address] = worker
            print(f"Registering worker {address}")
        return worker

    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        # Queue to broker and service waiting lists
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        self.dispatch(worker.service, None)

    def purge_workers(self):
        """Look for & kill expired workers.

        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.waiting:
            w = self.waiting[0]
            if w.expiry < time.time():
                logging.info("I: deleting expired worker: %s", w.identity)
                self.delete_worker(w,False)
                self.waiting.pop(0)
            else:
                break

    def serve(self):
        print(f'Starting ServiceQueue on {self.addr}')
        while True:
            try:
                msg = self.recv()
                if msg:  
                    print('got message: ', msg)                                                                          
                    # parse message
                    # if client
                    # msg ~ [sender identity, header, service, msg]
                    # if worker
                    # msg ~ [sender identity, header, comm_type, service, <client id>, msg]
                    
                    sender = msg.pop(0)
                    header = msg.pop(0)
                    
                    # got a msg from a client
                    if (header == COMM_HEADER_CLIENT):
                        print('client message')
                        # self.process_client(sender, msg)
                        if len(msg) == 2:
                            service, msg = msg
                            # add sender to msg
                            msg = [sender, msg]
                            # check if service exists
                            if service in self.services:
                                self.dispatch(service, msg)                    
                            else:
                                # reply to the client
                                self.send([sender, "Service does not exist"])
                        else:
                            print("Invalid message from client")

                    # got a msg from a worker                    
                    elif (header == COMM_HEADER_WORKER):
                        print('worker message')
                        
                        comm_type = msg.pop(0)
                        service = msg.pop(0)
                        
                        # make sure the worker exists
                        worker = self.require_worker(sender)
                        # attribute its service
                        worker.service = self.require_service(service)
                        
                        # set the worker to be waiting
                        self.worker_waiting(worker)

                        # reply to heartbeat
                        if comm_type == COMM_TYPE_HEARTBEAT:
                            msg = [sender, COMM_TYPE_HEARTBEAT, COMM_MSG_HEARTBEAT]
                            self.send(msg)

                        # send back to the client                            
                        elif comm_type == COMM_TYPE_REP:
                            # here, now message should be like [client id, msg]
                            # to route the reply to the appropriate client
                            if len(msg) == 2:
                                self.send(msg)
                            else:
                                print("Wrong format reply message from worker")                            
                        else:
                            print('Unknown message from worker')

                        # self.process_worker(sender, msg)
                    else:
                        raise Exception("Invalid message header")
                # delete expired workers
                # self.purge_workers()
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




                # client = msg[0]
                # send back a reply
                # print('reply')
                # test.update({client:'ola'})

                # msg = ['reply from queue']

                # print(test)
                # client = "sfjdslivhbsd"
                # msg = [client, '', C_CLIENT, "service"] + msg
                # msg = encode(msg)
                # socket.send_multipart(msg)


        except KeyboardInterrupt:
            break # Interrupted


if __name__ == '__main__':
    main()
