from random import randint
import time
from custom_zmq import ZMQR, ZMQReliableQueueWorker, COMM_HEARTBEAT_INTERVAL, create_identity
from custom_zmq import COMM_HEADER_WORKER, COMM_HEADER_CLIENT, COMM_TYPE_HEARTBEAT, COMM_TYPE_REP, COMM_TYPE_REQ, COMM_MSG_HEARTBEAT

import zmq
import json

class ZMQServiceQueueWorker(ZMQR):
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
        print(f"Service {self.service} worker {self.identity} ready")
        self.ping_t = time.time()
        self.pong_t = time.time()
        self.queue_alive = False
        self.send_heartbeat()

    def send_heartbeat(self):
        self.send_multipart([COMM_HEADER_WORKER, COMM_TYPE_HEARTBEAT, self.service, COMM_MSG_HEARTBEAT])

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
                print('got a request for work')                
                clientid, msg = msg[0], msg[1]
            
            elif comm_type == COMM_TYPE_HEARTBEAT:
                msg = msg.pop(0)
                if msg != COMM_MSG_HEARTBEAT:
                    print(f"Unknown heartbeat message from queue")                
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
                self.send_heartbeat()
        return clientid, msg

    def send_work(self, clientid:str, msg:str):
        return self.send_multipart([COMM_HEADER_WORKER, COMM_TYPE_WORKER_REPLY, self.service, clientid, msg])

if __name__ == '__main__':
    addr = "tcp://127.0.0.1:5555"
    ctx = zmq.Context()
    s = ZMQServiceQueueWorker(ctx, "echo")
    s.connect(addr)

    while True:
        clientid, msg = s.recv_work()
        if msg:
            print('Work received: ', msg)    
            print('sleeping')
            # time.sleep(COMM_HEARTBEAT_INTERVAL*2)
            print('send reply')        
            rep = {'status':'ok', 'ola':1.5}
            rep = json.dumps(rep)
            s.send_work(clientid, rep)

    s.close()
    ctx.term()
