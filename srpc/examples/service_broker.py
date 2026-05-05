
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
from custom_zmq import ZMQR, ZMQServiceBroker


def main():
    """create and start new broker"""

    addr = "tcp://127.0.0.1:4446"

    q = ZMQServiceBroker(addr)
    q.serve()
    return None

if __name__ == '__main__':
    main()
