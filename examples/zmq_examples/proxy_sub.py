



import sys
import time

from random import randint

import zmq

from custom_zmq import ZMQSub



ctx = zmq.Context.instance()

s = ZMQSub(ctx)
addr = "tcp://127.0.0.1:5556"
s.connect(addr)

topic = "%03d" % randint(0,999)
print('sub to all')
s.subscribe(topic)

while True:
    topic, msg = s.recv()
    if topic is not None:
        print(topic, msg)

s.close()
ctx.term()
