
import threading
import time
from srpc import SRPCClient


class StreamerClientExample(SRPCClient):
    def __init__(self, broker_addr:str = None, proxy_pub_addr:str = None, timeo:int = 1, last_msg_only:bool = True):
        SRPCClient.__init__(self, broker_addr = broker_addr, proxy_pub_addr = proxy_pub_addr, timeo = timeo, last_msg_only = last_msg_only)# , no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    def set_m(self, m):
        return self.invoque(service='StreamerExample', method = 'set_m', args = [], kwargs = {'m':m}, close = False)

    def set_s(self, s):
        return self.invoque(service='StreamerExample', method = 'set_s', args = [], kwargs = {'s':s}, close = False)


if __name__ == '__main__':
    client = StreamerClientExample(last_msg_only = False)
    client.subscribe("somekey")

    print(client.set_m(10))

    c = 0
    while True:
        print(client.listen())
        c+=1
        if c>5:
            break
    print(client.set_m(-10))
    c = 0
    while True:
        print(client.listen())
        c+=1
        if c>5:
            break

    client.close()
