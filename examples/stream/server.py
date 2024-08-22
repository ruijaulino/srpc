import threading
import time
import numpy as np
from srpc import SRPCServer

class StreamerServerExample(SRPCServer):
    def __init__(self, rep_addr:str = None, pub_addr:str = None, service_name:str = "StreamerExample"):

        SRPCServer.__init__(
                            self, 
                            name = service_name, 
                            rep_addr = rep_addr,
                            pub_addr = pub_addr,
                            # registry_addr = registry_addr,
                            timeo = 1, # this will be overriden by the reliable worker
                            n_workers = 1, 
                            thread_safe = True, 
                            lvc = True,
                            clear_screen = False
                            )
        self.th = None
        self.m = 0
        self.s = 1

    def stream(self):
        while not self.stop_event.isSet():
            self.publish("somekey", np.random.normal(self.m, self.s))
            time.sleep(0.5)

    # as an example, override the close
    def close(self):
        self._close()
        self.th.join()
        print('Done overriden close')

    def start(self):
        self.th = threading.Thread(target = self.stream, daemon = True)
        self.th.start()

    def set_m(self, m):
        self.m = m
        return 1
    
    def set_s(self, s):
        self.s = s
        return 1

if __name__ == '__main__':
    server = StreamerServerExample(rep_addr = "tcp://127.0.0.1:6420", pub_addr = "tcp://127.0.0.1:6421")  
    server.serve()
