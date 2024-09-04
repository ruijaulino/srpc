import sys
import time
import threading
from random import randint
import zmq
from custom_zmq import ZMQPub



ctx = zmq.Context.instance()

s = ZMQPub(ctx)
s.connect("tcp://127.0.0.1:5557")
time.sleep(2)
# publish many right away
for topic_nbr in range(1000):
    topic = "%03d" % topic_nbr
    s.publish(topic, "Save Roger")

while True:
    # Send one random update per second
    try:
        time.sleep(0.1)  
        s.publish("%03d" % randint(0,999), "Off with his head!")          
    except KeyboardInterrupt:
        print("interrupted")
        break

s.close()
ctx.term()
