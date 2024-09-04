from random import randint
import time
from custom_zmq import ZMQR, ZMQServiceBroker


def main():
    """create and start new broker"""

    addr = "tcp://127.0.0.1:5555"

    q = ZMQServiceBroker(addr)
    q.serve()
    return None

if __name__ == '__main__':
    main()
