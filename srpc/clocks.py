# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
import pytz
import time
import zmq
import pandas as pd
try:
    from .server import SRPCServer
    from .utils import SRPCTopic   
    from .client import SRPCClient
    from .custom_zmq import ZMQPub
    from .defaults import REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG, REGISTRY_ADDR
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR

except ImportError:
    from server import SRPCServer
    from utils import SRPCTopic
    from client import SRPCClient
    from custom_zmq import ZMQPub
    from defaults import REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG, REGISTRY_ADDR
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR



def today_business_day():
    # Get today's date
    today = pd.Timestamp.today()
    # Check if today is a business day
    return pd.bdate_range(start=today, end=today).size == 1


def clocks(proxy_sub_addr:str = None, minute_clocks_tz:list = ['Europe/Lisbon', 'US/Eastern','Asia/Tokyo'], offsets:list = [5,10,15,20,25,30,35,40,45,50,55]):
    
    print('Starting clocks')
    if not proxy_sub_addr: proxy_sub_addr = PROXY_SUB_ADDR
        
    ctx = zmq.Context()
    pub_socket = ZMQPub(ctx = ctx)
    pub_socket.connect(proxy_sub_addr)
    
    pacing = 0.001
    pacing_th = 0.1 # must be larger than pacing

    # send msgs
    pubs = []
    pubs_max_size = max(1, 2*len(minute_clocks_tz)*len(offsets))
    while True:                                    
        try:
            if today_business_day():
                for tz in minute_clocks_tz:
                    for offset in offsets:
                        # code to publish a clock
                        now = dt.datetime.now(pytz.timezone(tz))
                        seconds = now.second
                        # old code when the minute was defined
                        minute = now.minute + 1
                        hour = now.hour
                        if minute == 60:
                            minute = 0
                            hour += 1
                        if hour == 24:
                            hour = 0
                        # Check if we are offset_seconds the target time
                        if (seconds >= 60 - offset - pacing_th and seconds <= 60 - offset + pacing_th):
                            topic = SRPCTopic('trigger', 'clock', tz, hour, minute, 'offset'+str(offset))
                            if topic.topic not in pubs:
                                pub_socket.publish(topic = topic.topic, msg = 'trigger')
                                pubs.append(topic.topic)
            time.sleep(pacing)
            # keep constant size
            pubs = pubs[-pubs_max_size:]
        except KeyboardInterrupt:
            print('Terminate clocks')
            break

    pub_socket.close()
    ctx.term()


def listen_clocks():
   
    # NOTE: if subscribing to more than one topic, set last_msg_only = False in case messages can come at the same time 
       
    client = SRPCClient(last_msg_only = False)

    time.sleep(1)
    
    topic = SRPCTopic('trigger', 'clock')
    client.subscribe(topic = topic.topic)
        
    while True:
        topic, msg = client.wait()
        print(dt.datetime.now(), topic, msg)
    client.close()
   
if __name__ == "__main__":





    print("Today is a business day." if is_business_day else "Today is not a business day.")

    # clocks()
    # listen_clocks()
