
import threading
import time
import numpy as np
from srpc import SRPCClient

class StreamerClientExample(SRPCClient):
    def __init__(self, host, port, sub_port = None):
        SRPCClient.__init__(self, host = host, port = port, sub_port = sub_port)    

    # there is a method .parse in SRPCClient that helps parse the response from SRPCServer's

    # basically here we are wrapping aroung the methods in the StreamServer to make the API similar
    def set_m(self, m):
        rep = self.call(method = 'set_m', args = [], kwargs = {'m':m}, close = False)
        return self.parse(rep)

    def set_s(self, s):
        rep = self.call(method = 'set_s', args = [], kwargs = {'s':s}, close = False)
        return self.parse(rep)        

if __name__ == '__main__':
    
    host = 'localhost'
    port = 6000

    client = StreamerClientExample(host, port, port+1)
    client.subscribe("random_number")
    print(client.set_m(10))
    c = 0
    while True:
        topic, msg = client.listen()
        if topic is not None:
            print(topic.topic, msg)
        c+=1
        if c>5:
            break
    print(client.set_m(-10))
    c = 0
    while True:
        if topic is not None:
            print(topic.topic, msg)
        c+=1
        if c>5:
            break

    client.close()