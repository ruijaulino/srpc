import time
import zmq
import threading
from custom_zmq import ZMQR, ZMQSimpleQueue

ctx = zmq.Context()
q = ZMQSimpleQueue(ctx, "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556", standalone = False)
q.start()
while True:
    try:
        time.sleep(0.1)
    except KeyboardInterrupt:
        print('detected KeyboardInterrupt')
        break
q.stop() 
ctx.term()       
exit(0)

