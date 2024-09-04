import zmq
import json
import threading
import time
import datetime as dt
import os
import threading
try:
    from .custom_zmq import ZMQServiceBroker, ZMQProxy
except ImportError:
    from custom_zmq import ZMQServiceBroker, ZMQProxy
try:
    from .defaults import SERVICES_BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from defaults import SERVICES_BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR

def proxy(pub_addr:str = None, sub_addr:str = None, topics_no_cache = ['trigger']):
    if not pub_addr: pub_addr = PROXY_PUB_ADDR
    if not sub_addr: sub_addr = PROXY_SUB_ADDR
    ZMQProxy(pub_addr = pub_addr, sub_addr = sub_addr, topics_no_cache = topics_no_cache)

def services_broker(addr:str = None, create_service_on_client:bool = False):
    if not addr: addr = SERVICES_BROKER_ADDR
    sb = ZMQServiceBroker(addr = addr, create_service_on_client = create_service_on_client)
    sb.serve()

if __name__ == '__main__':
    
    # launch both
    th_proxy = threading.Thread(target = proxy)
    th_proxy.start()

    th_sb = threading.Thread(target = services_broker)
    th_sb.start()


