import threading
import time
import numpy as np
from srpc import SRPCServer


# make the method fun thread safe by using locks


class StreamerServerExample(SRPCServer):
    def __init__(self, service_name, host, port, pub_port = None, n_workers = 1):
        SRPCServer.__init__(self, name = service_name, host = host, port = port, pub_port = pub_port, n_workers = n_workers, thread_safe = False)
        self.lock = threading.Lock()
        self.x = 0

    # as an example, override the close
    def close(self):
        self.srpc_close()
        
    def start(self):
        pass

    def fun(self):
        # several workers can access x at the same time
        # make it thread safe
        with self.lock:
            self.x += 1
            print('in fun: ', self.x)
        time.sleep(5)
        print('out of fun')
        return 0

def test_server(host, port, n_workers = 5):
    server = StreamerServerExample(service_name = 'stream_example', host = host, port = port, pub_port = port+1, n_workers = n_workers)  
    server.serve()    

if __name__ == '__main__':
    host = 'localhost'
    port = 6000
    # test_client(host, port)
    test_server(host, port)
