import sys
import time
import threading
from random import randint
import zmq
from custom_zmq import ZMQP



ctx = zmq.Context.instance()

s = ZMQP(ctx, lvc = True)
s.bind("tcp://127.0.0.1:5556")

# publish many right away
for topic_nbr in range(1000):
    s.publish("%03d" % topic_nbr, "Save Roger")

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
exit(0)


publisher = ctx.socket(zmq.XPUB)
publisher.bind("tcp://127.0.0.1:5556")
# Ensure subscriber connection has time to complete
timeo = 1000

time.sleep(1)



# Send out all 1,000 topic messages
for topic_nbr in range(1000):
    if (publisher.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
        publisher.send_multipart([
            b"%03d" % topic_nbr,
            b"Save Roger",
        ])


while True:
    # Send one random update per second
    try:
        time.sleep(1)
        if (publisher.poll(timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:
            
            publisher.send_multipart([
                b"%03d" % randint(0,999),
                b"Off with his head!",
            ])
        if (publisher.poll(timeo, zmq.POLLIN) & zmq.POLLIN) != 0:
            out = publisher.recv()
            print('can read!')
            if out[0] == 1:
                print('SUB TO: ', out[1:])
                print()
            else:
                print('UNSUB FROM: ', out[1:])
                print()


    except KeyboardInterrupt:
        print("interrupted")
        break
    