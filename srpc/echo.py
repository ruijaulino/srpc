# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
import time
try:
    from .server import SRPCServer
    from .utils import SRPCTopic   
    from .client import SRPCClient
    from .defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

except ImportError:
    from server import SRPCServer
    from utils import SRPCTopic
    from client import SRPCClient
    from defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

try:
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR


class Echo(SRPCServer):
    def __init__(
        self,
        broker_addr=None,
        proxy_sub_addr=None,
        service_name="Echo",
        delay = 5
    ):
        super().__init__(
            name=service_name,
            broker_addr=broker_addr,
            proxy_sub_addr=proxy_sub_addr,
            timeo=1,
            n_workers=1,
            thread_safe=False,
            clear_screen=True,
        )

        self.delay = delay


    def echo(self, msg = ''):
        print('Calling echo')
        time.sleep(self.delay)
        return msg


class EchoClient(SRPCClient):
    def __init__(self, srpc_client:SRPCClient, service_name:str = 'Echo', timeo:int = 10, last_msg_only:bool = True, no_rep_msg:str = None, no_req_msg:str = None):        
        self.service_name = service_name
        self.srpc_client = srpc_client
        if no_rep_msg: self.srpc_client.no_rep_msg = no_rep_msg
        if no_req_msg: self.srpc_client.no_req_msg = no_req_msg

    def echo(self, msg, collect = True):
        return self.srpc_client.invoque(service = self.service_name, method = 'echo', collect = collect, args = [], kwargs = {'msg':msg}, close = False)
        
    def collect(self):
        return self.srpc_client.collect()

def test_server():

    SERVICES_BROKER_ADDR = f"tcp://192.168.2.152:5000"
    SERVICES_PROXY_PUB_ADDR = f"tcp://192.168.2.152:5001"
    SERVICES_PROXY_SUB_ADDR = f"tcp://192.168.2.152:5002"



    server = Echo(broker_addr=SERVICES_BROKER_ADDR, proxy_sub_addr = SERVICES_PROXY_SUB_ADDR, delay = 3)  
    server.serve()


def test_client():
    SERVICES_BROKER_ADDR = f"tcp://192.168.2.152:5000"
    SERVICES_PROXY_PUB_ADDR = f"tcp://192.168.2.152:5001"
    SERVICES_PROXY_SUB_ADDR = f"tcp://192.168.2.152:5002"

    srpc_client = SRPCClient(broker_addr = SERVICES_BROKER_ADDR, proxy_pub_addr = SERVICES_PROXY_PUB_ADDR, timeo = 60)

    client = EchoClient(srpc_client)

    print(client.echo('ola!', collect = False))
    print(client.collect())


    srpc_client.close()
   

if __name__ == "__main__":
    # test_server()
    test_client()
