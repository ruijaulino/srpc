# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
import pytz
import time
try:
    from .server import SRPCServer
    from .wrappers import SRPCTopic   
    from .client import SRPCClient
    from .defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

except ImportError:
    from server import SRPCServer
    from wrappers import SRPCTopic
    from client import SRPCClient
    from defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

# Triggers with built in clocks
# clocks are published with the topic
# SRPCTopic('trigger', 'clock', tz, hour, minute, 'offset'+str(offset))

class Trigger(SRPCServer):
    def __init__(self, host:str, port:int, pub_port:int = None, registry_host:str = REGISTRY_HOST, registry_port:int = REGISTRY_PORT, minute_clocks_tz:list = [], offsets:list = [], service_name:str = 'Triggers'):                
        SRPCServer.__init__(self, name = service_name, host = host, port = port, pub_port = pub_port, registry_host = registry_host, registry_port = registry_port)
        self.minute_clocks_tz = minute_clocks_tz
        self.offsets = offsets
        self.pacing = 0.05

    def clocks(self):
        # sent msgs
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
                    if seconds >= 60 - offset:
                        topic = SRPCTopic('trigger', 'clock', tz, hour, minute, 'offset'+str(offset))
                        if topic.topic not in pubs:
                            print('pub: ', topic.topic)
                            self.publish(topic = topic, value = 'trigger')
                            pubs.append(topic.topic)
            time.sleep(self.pacing)
            # keep constant size
            pubs = pubs[-pubs_max_size:]

    # as an example, override the close
    def close(self):
        self.srpc_close()
        self.clocks_th.join()

    def start(self):
        self.clock_th = threading.Thread(target = self.clocks, daemon = True)
        self.clock_th.start()


class TriggerClient(SRPCClient):
    def __init__(self, host, port, sub_port = None, recvtimeo:int = 1000, sub_recvtimeo:int = 1000, sndtimeo:int = 100, last_msg_only:bool = True, no_rep_msg = NO_REP_MSG, no_req_msg = NO_REQ_MSG):
        SRPCClient.__init__(self, host = host, port = port, sub_port = sub_port, recvtimeo = 1000, sub_recvtimeo = 1000, sndtimeo = 100, last_msg_only = last_msg_only, no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    # must be subscribed before
    # waits for a topic (could just increase the timeout)
    def wait(self):
        while True:
            topic, msg = self.listen()
            if topic is not None:
                return topic, msg

    def publish(self, topic:SRPCTopic, value:str):
        rep = self.call(method = 'publish', args = [], kwargs = {'topic':topic.topic, 'value':value}, close = False)
        return self.parse(rep)


def test_server():
    server = Trigger(host = 'localhost', port = 5550, pub_port = 5551, minute_clocks_tz = ['Europe/Lisbon', 'US/Eastern'], offsets = [5,10,20,30], service_name = 'Triggers')  
    server.serve()

def test_client():
   
    client = TriggerClient(host = 'localhost', port = 5550, sub_port = 5551)    
    
    timezone = 'Europe/Lisbon'
    topic = SRPCTopic('trigger', 'clock', timezone)

    client.subscribe(topic = topic)
    time.sleep(1)
    timezone = 'US/Eastern'
    topic = SRPCTopic('trigger', 'clock', timezone)
    client.subscribe(topic = topic)# , unsubscribe = False)
    
    while True:
        topic, msg = client.wait()
        print(dt.datetime.now(), topic, msg)
    
    client.close()
   
if __name__ == "__main__":
    # test_server()
    test_client()
