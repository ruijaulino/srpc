import threading
import time
import numpy as np
from srpc import SRPCServer

# by inheriting from SRCPServer we can easily make a class as a service
# clients call class methods

class StreamerServerExample(SRPCServer):
    def __init__(self, service_name, host, port, pub_port = None):
        SRPCServer.__init__(self, name = service_name, host = host, port = port, pub_port = None)
        self.th = None
        self.m = 0
        self.s = 1

    def stream(self):
        while not self.stop_event.isSet():
            self.publish("random_number", np.random.normal(self.m, self.s))
            time.sleep(0.5)

    # as an example, override the close
    # this is called when the code is shutdown by the user
    def close(self):
        self.srpc_close()
        self.th.join()
        print('Done overriden close')

    # this is called when we call the method .serve()
    def start(self):
        self.th = threading.Thread(target = self.stream, daemon = True)
        self.th.start()

    def set_m(self, m):
        self.m = m

    def set_s(self, s):
        self.s = s

def test_server(host, port):
    server = StreamerServerExample(service_name = 'stream_example', host = host, port = port, pub_port = port+1)  
    server.serve()    



if __name__ == '__main__':
    host = 'localhost'
    port = 6000
    # test_client(host, port)
    test_server(host, port)
