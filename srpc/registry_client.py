import zmq
import json
import threading
import time
import datetime as dt
import os
try:
    from .utils import clear_screen
    from .client import SRPCClient
    from .custom_zmq import ZMQR, ZMQP
except ImportError:
    from utils import clear_screen
    from client import SRPCClient  
    from custom_zmq import ZMQR, ZMQP
try:
    from .defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT
except ImportError:
    from defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT

class RegistryClient(SRPCClient):
    def __init__(self, req_addr:str, timeo:int = 1):        
        SRPCClient.__init__(self, req_addr = req_addr, timeo = timeo)

    def heartbeat(self, info:dict):
        return self.invoque(method = 'heartbeat', args = [], kwargs = {'info':info}, close = False)

    def services(self):
        return self.invoque(method = 'services', args = [], kwargs = {}, close = False)

if __name__ == "__main__":
    REGISTRY_HOST = '192.168.2.152'
    REGISTRY_PORT = 4000
    REGISTRY_ADDR = f"tcp://{REGISTRY_HOST}:{REGISTRY_PORT}"

    registry = Registry(REGISTRY_ADDR)
    registry.serve()
    

