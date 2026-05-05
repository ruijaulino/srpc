

import os
import sys

# add ccsys dir (two levels above)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
sys.path.append(current_dir)


from random import randint
import time
from custom_zmq import ZMQR, ZMQServiceBrokerClient
import zmq
import json


if __name__ == '__main__':
    addr = "tcp://127.0.0.1:4446"
    ctx = zmq.Context()
    s = ZMQServiceBrokerClient(ctx)
    s.connect(addr)

    # req/rep
    service = "echo"
    msg = "ola"
    #for i in range(20):
    status_1 = s.req(service, msg)
    print('status: ', status_1)
    
    status_2 = s.req(service, msg)
    print('status: ', status_2)

    rep = s.rep(timeo = 5)
    
    print('Got reply: ', rep)

    rep = s.rep()
    
    print('Got reply: ', rep)



    s.close()

    ctx.destroy()
