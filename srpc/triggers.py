# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
import pytz
import time
try:
    from .server import SRPCServer
    from .utils import SRPCTopic   
    from .client import SRPCClient
    from .defaults import REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG, REGISTRY_ADDR

except ImportError:
    from server import SRPCServer
    from utils import SRPCTopic
    from client import SRPCClient
    from defaults import REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG, REGISTRY_ADDR

# Triggers with built in minute clocks
# clocks are published with the topic
# SRPCTopic('trigger', 'clock', tz, hour, minute, 'offset'+str(offset))
class Triggers(SRPCServer):
    def __init__(self, rep_addr:str, pub_addr:str, registry_addr:str = None, minute_clocks_tz:list = [], offsets:list = [], service_name:str = 'Triggers'):                
        SRPCServer.__init__(
                            self, 
                            name = service_name, 
                            rep_addr = rep_addr,
                            pub_addr = pub_addr,
                            registry_addr = registry_addr,
                            timeo = 1, # this will be overriden by the reliable worker
                            n_workers = 1, 
                            thread_safe = False, 
                            lvc = False, # do not do this!
                            clear_screen = False
                            )
        self.minute_clocks_tz = minute_clocks_tz
        self.offsets = offsets
        self.pacing = 0.001
        self.pacing_th = 0.1 # must be larger than pacing

    def clocks(self):
        # send msgs
        pubs = []
        pubs_max_size = max(1, 2*len(self.minute_clocks_tz)*len(self.offsets))
        while not self.stop_event.isSet():                                    
            for tz in self.minute_clocks_tz:
                for offset in self.offsets:
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
                    if (seconds >= 60 - offset - self.pacing_th and seconds <= 60 - offset + self.pacing_th):
                        topic = SRPCTopic('trigger', 'clock', tz, hour, minute, 'offset'+str(offset))
                        if topic.topic not in pubs:
                            self.publish(topic = topic.topic, msg = 'trigger')
                            pubs.append(topic.topic)
            time.sleep(self.pacing)
            # keep constant size
            pubs = pubs[-pubs_max_size:]

    # as an example, override the close
    def close(self):
        self._close()
        self.clock_th.join()

    def start(self):
        self.clock_th = threading.Thread(target = self.clocks, daemon = True)
        self.clock_th.start()


class TriggersClient(SRPCClient):
    def __init__(self, req_addr:str, sub_addr:str, timeo:int = 1, last_msg_only:bool = True, no_rep_msg = NO_REP_MSG, no_req_msg = NO_REQ_MSG):                
        SRPCClient.__init__(self, req_addr = req_addr, sub_addr = sub_addr, timeo = timeo, last_msg_only = last_msg_only, no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    # must be subscribed before
    # waits for a topic (could just increase the timeout)
    def wait(self):
        while True:
            topic, msg = self.listen()
            if topic is not None:
                return topic, msg

    def publish(self, topic:str, msg:str):
        return self.invoque(method = 'publish', args = [], kwargs = {'topic':topic, 'msg':msg}, close = False)

def test_server():
    server = Triggers(rep_addr = "tcp://127.0.0.1:6420", pub_addr = "tcp://127.0.0.1:6421", minute_clocks_tz = ['Europe/Lisbon', 'US/Eastern'], offsets = [5,10,20,30], service_name = 'Triggers')  
    server.serve()

def test_client():
   
    # NOTE: if subscribing to more than one topic, set last_msg_only = False in case messages can come at the same time 
    client = TriggersClient(req_addr = "tcp://127.0.0.1:6420", sub_addr = "tcp://127.0.0.1:6421", last_msg_only = False)        
    
    timezone = 'Europe/Lisbon'
    topic = SRPCTopic('trigger', 'clock', timezone)
    client.subscribe(topic = topic.topic)
    time.sleep(1)
    
    timezone = 'US/Eastern'
    topic = SRPCTopic('trigger', 'clock', timezone)
    client.subscribe(topic = topic.topic)# , unsubscribe = False)
    
    while True:
        topic, msg = client.wait()
        print(dt.datetime.now(), topic, msg)
    client.close()
   
if __name__ == "__main__":
    test_server()
    # test_client()
