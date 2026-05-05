

# import trdm
import os
import sys

# add ccsys dir (two levels above)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
sys.path.append(current_dir)


from random import randint
import time
from custom_zmq import ZMQR, ZMQServiceBrokerWorker

import zmq
import json

if __name__ == '__main__':
    addr = "tcp://127.0.0.1:4446"
    ctx = zmq.Context()
    s = ZMQServiceBrokerWorker(ctx, "echo")
    s.connect(addr)

    while True:
        clientid, reqid, msg = s.recv_work()
        if msg:
            print('Work received: ', msg)    
            # print('sleeping')
            # time.sleep(COMM_HEARTBEAT_INTERVAL*2)
            print('send reply')        
            # rep = {'status':'ok', 'ola':1.5}
            # rep = json.dumps(rep)
            s.send_work(clientid, reqid, msg)

    s.close()
    ctx.term()
