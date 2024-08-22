
import threading
import time
import numpy as np
from srpc import SRPCClient


class StreamerClientExample(SRPCClient):
    def __init__(self, req_addr:str, sub_addr:str, timeo:int = 1, last_msg_only:bool = True):                
        SRPCClient.__init__(self, req_addr = req_addr, sub_addr = sub_addr, timeo = timeo, last_msg_only = last_msg_only)# , no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    def set_m(self, m):
        return self.invoque(method = 'set_m', args = [], kwargs = {'m':m}, close = False)

    def set_s(self, s):
        return self.invoque(method = 'set_s', args = [], kwargs = {'s':s}, close = False)


if __name__ == '__main__':
    client = StreamerClientExample(req_addr = "tcp://127.0.0.1:6420", sub_addr = "tcp://127.0.0.1:6421", last_msg_only = False)        
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
