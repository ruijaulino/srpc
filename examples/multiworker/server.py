from srpc import SRPCServer
import time

class MultiworkerServer(SRPCServer):
    def __init__(
                self, 
                broker_addr:str = None, 
                proxy_sub_addr:str = None,
                registry_addr:str = None,
                n_workers:int = 1, 
                service_name:str = 'ExampleMultiworker'
                ):
        
        # init SRPCServer
        SRPCServer.__init__(
                            self, 
                            name = service_name, 
                            broker_addr = broker_addr,
                            proxy_sub_addr = proxy_sub_addr,
                            timeo = 1, 
                            n_workers = n_workers, 
                            thread_safe = False, 
                            clear_screen = False,
                            worker_info = True
                            )

    def somemethod(self, param):
        print('Got param: ', param)
        time.sleep(1)
        return 1


if __name__ == '__main__':

    # services broker
    SERVICES_BROKER_ADDR = f"tcp://192.168.2.152:5000"
    SERVICES_PROXY_PUB_ADDR = f"tcp://192.168.2.152:5001"
    SERVICES_PROXY_SUB_ADDR = f"tcp://192.168.2.152:5002"

    REGISTRY_HOST = '192.168.2.152'
    REGISTRY_PORT = 4000
    REGISTRY_ADDR = f"tcp://{REGISTRY_HOST}:{REGISTRY_PORT}"


    n_workers = 10

    mw = MultiworkerServer(SERVICES_BROKER_ADDR, SERVICES_PROXY_SUB_ADDR, REGISTRY_ADDR, n_workers)
    mw.serve()




