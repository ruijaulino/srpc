import threading
import time
import random
from srpc import SRPCServer, rpc_method

class StreamerServerExample(SRPCServer):
    def __init__(self, broker_addr:str = None, proxy_sub_addr:str = None, service_name:str = "StreamerExample"):

        SRPCServer.__init__(
                            self,
                            name = service_name,
                            broker_addr = broker_addr,
                            proxy_sub_addr = proxy_sub_addr,
                            # registry_addr = registry_addr,
                            timeo = 1, # this will be overriden by the reliable worker
                            n_workers = 1,
                            thread_safe = True,
                            clear_screen = False
                            )
        self.th = None
        self.m = 0
        self.s = 1

    def stream(self):
        while not self.stop_event.is_set():
            self.publish("somekey", random.gauss(self.m, self.s))
            time.sleep(0.5)

    # as an example, override the close
    def close(self):
        self.stop_event.set()
        if self.th is not None:
            self.th.join()
        self._close()

    def start(self):
        self.th = threading.Thread(target = self.stream, daemon = True)
        self.th.start()

    @rpc_method
    def set_m(self, m):
        self.m = m
        return 1

    @rpc_method
    def set_s(self, s):
        self.s = s
        return 1

if __name__ == '__main__':
    server = StreamerServerExample()
    server.serve()
