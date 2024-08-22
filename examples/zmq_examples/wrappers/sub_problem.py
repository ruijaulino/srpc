


# import sys
# import time

# from random import randint

# import zmq

# def main(url=None):
#     ctx = zmq.Context.instance()
#     subscriber = ctx.socket(zmq.SUB)
#     if url is None:
#         url = "tcp://127.0.0.1:5556"
#     subscriber.connect(url)

#     subscription = b"%03d" % randint(0,999)
#     subscriber.setsockopt(zmq.SUBSCRIBE, subscription)

#     while True:
#         topic, data = subscriber.recv_multipart()
#         assert topic == subscription
#         print(data)

# if __name__ == '__main__':
#     main(sys.argv[1] if len(sys.argv) > 1 else None)






import sys
import time

from random import randint

import zmq

from custom_zmq import ZMQS



ctx = zmq.Context.instance()

s = ZMQS(ctx)
addr = "tcp://127.0.0.1:5556"
s.connect(addr)

topic = "%03d" % randint(0,999)
s.subscribe(topic)

while True:
    topic, msg = s.recv()
    if topic is not None:
        print(topic, msg)

s.close()
ctx.term()
