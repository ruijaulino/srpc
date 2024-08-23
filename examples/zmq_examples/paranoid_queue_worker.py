from random import randint
import time
from custom_zmq import ZMQR, ZMQReliableQueueWorker, COMM_HEARTBEAT_INTERVAL
import zmq
import json

addr = "tcp://127.0.0.1:5556"
ctx = zmq.Context()
s = ZMQReliableQueueWorker(ctx)
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
