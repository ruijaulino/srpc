from random import randint
import time
from custom_zmq import ZMQR, ZMQServiceBrokerClient
import zmq
import json


if __name__ == '__main__':
    addr = "tcp://127.0.0.1:5555"
    ctx = zmq.Context()
    s = ZMQServiceBrokerClient(ctx)
    s.connect(addr)

    # req/rep
    service = "echo"
    msg = "ola"
    rep = s.request(service, msg)
    print('Got reply: ', rep)

    s.close()

    ctx.destroy()
