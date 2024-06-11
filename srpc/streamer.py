import threading
import time
import numpy as np
try:
    from .server import SRPCServer
    from .client import SRPCClient
except ImportError:
    from server import SRPCServer
    from client import SRPCClient



class StreamerServerExample(SRPCServer):
    def __init__(self, service_name, host, port, pub_port = None):
        SRPCServer.__init__(self, name = service_name, host = host, port = port, pub_port = None)
        self.th = None
        self.m = 0
        self.s = 1

    def stream(self):
        while not self.stop_event.isSet():
            print('pub ', self.m, self.s)
            self.publish("somekey", np.random.normal(self.m, self.s))
            time.sleep(0.5)

    def start_stream(self):
        self.th = threading.Thread(target = self.stream, daemon = True)
        self.th.start()

    def set_m(self, m):
        self.m = m

    def set_s(self, s):
        self.s = s


class StreamerClientExample(SRPCClient):
    def __init__(self, host, port, sub_port = None):
        SRPCClient.__init__(self, host = host, port = port, sub_port = sub_port)    

    def parse(self, rep):
        if rep.get('status') == "ok":
            return rep.get('value')
        else:
            return rep.get('msg')

    def set_m(self, m):
        rep = self.call(method = 'set_m', args = [], kwargs = {'m':m}, close = False)
        return self.parse(rep)

    def set_s(self, s):
        rep = self.call(method = 'set_s', args = [], kwargs = {'s':s}, close = False)
        return self.parse(rep)        

def test_client(host, port):
    client = StreamerClientExample(host, port, port+1)
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

def test_server(host, port):
    server = StreamerServerExample(service_name = 'stream', host = host, port = port, pub_port = port+1)  
    server.start_stream()
    server.serve()    

if __name__ == '__main__':
    host = 'localhost'
    port = 6000
    test_client(host, port)
    # test_server(host, port)
