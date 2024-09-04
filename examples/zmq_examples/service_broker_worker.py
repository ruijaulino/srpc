from random import randint
import time
from custom_zmq import ZMQR, ZMQServiceBrokerWorker

import zmq
import json

if __name__ == '__main__':
    addr = "tcp://127.0.0.1:5555"
    ctx = zmq.Context()
    s = ZMQServiceBrokerWorker(ctx, "echo")
    s.connect(addr)

    while True:
        clientid, msg = s.recv_work()
        if msg:
            print('Work received: ', msg)    
            # print('sleeping')
            # time.sleep(COMM_HEARTBEAT_INTERVAL*2)
            print('send reply')        
            # rep = {'status':'ok', 'ola':1.5}
            # rep = json.dumps(rep)
            s.send_work(clientid, msg)

    s.close()
    ctx.term()
