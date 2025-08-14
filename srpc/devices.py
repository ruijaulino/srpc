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
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR

def proxy(pub_addr:str = None, sub_addr:str = None, topics_no_cache = ['trigger'], stop_event:threading.Event = None):
    if not pub_addr: pub_addr = PROXY_PUB_ADDR
    if not sub_addr: sub_addr = PROXY_SUB_ADDR
    ZMQProxy(pub_addr = pub_addr, sub_addr = sub_addr, topics_no_cache = topics_no_cache, stop_event = stop_event)

def broker(addr:str = None, stop_event:threading.Event = None):
    if not addr: addr = BROKER_ADDR
    sb = ZMQServiceBroker(addr = addr, stop_event = stop_event)
    sb.serve()

def devices(broker_addr:str = None, pub_addr:str = None, sub_addr:str = None, topics_no_cache = ['trigger']):
    stop_event = threading.Event()
    th_proxy = threading.Thread(target = proxy, kwargs = {'pub_addr':pub_addr, 'sub_addr':sub_addr, 'topics_no_cache':topics_no_cache, 'stop_event':stop_event})
    th_proxy.start()
    th_sb = threading.Thread(target = broker, kwargs = {'addr':broker_addr, 'stop_event':stop_event})
    th_sb.start()
    while True:
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            stop_event.set()
            break
    th_sb.join()
    th_proxy.join()

if __name__ == '__main__':    
    broker_addr = None
    pub_addr = None
    sub_addr = None
    devices()
