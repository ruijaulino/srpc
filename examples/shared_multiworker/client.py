
import threading
import time
import numpy as np
from srpc import SRPCClient

class StreamerClientExample(SRPCClient):
    def __init__(self, host, port, sub_port = None, recvtimeo = 10000, sndtimeo = 10000):
        SRPCClient.__init__(self, host = host, port = port, sub_port = sub_port, recvtimeo = 10000, sndtimeo = 10000)    

    def fun(self):
        rep = self.call(method = 'fun', args = [], kwargs = {}, close = False)
        return self.parse(rep)


def make_request():
    host = 'localhost'
    port = 6000

    client = StreamerClientExample(host, port, port+1, recvtimeo = 10000, sndtimeo = 10000)
    
    out = client.fun()
    print(out)

    client.close()


if __name__ == '__main__':
    
    n_requests = 3
    ths = []
    for i in range(n_requests):
        th = threading.Thread(target = make_request, daemon = True)
        th.start()
        ths.append(th)
    for th in ths:
        th.join()
    print('Done..')