from random import randint
import time
import zmq
import custom_zmq
from custom_zmq import ZMQR, ZMQSimpleQueueWorker


addr = "tcp://127.0.0.1:5556"
ctx = zmq.Context()

identity = "%04X-%04X" % (randint(0, 0x10000), randint(0,0x10000))

s = ZMQSimpleQueueWorker(ctx, identity = identity)
s.connect(addr)

while True:
    try:
        clientid, msg = s.recv_work()
        if msg:
            print('Worker received: ', msg)            
            s.send_work(clientid, "work is done")
            
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        break

s.close()
ctx.term()




