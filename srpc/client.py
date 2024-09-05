import zmq
import json
import time

try:
    from .custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker, ZMQSub, ZMQServiceBrokerClient
    from .utils import build_server_response, OK_STATUS, ERROR_STATUS
    from .defaults import NO_REP_MSG, NO_REQ_MSG
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker, ZMQSub, ZMQServiceBrokerClient
    from utils import build_server_response, OK_STATUS, ERROR_STATUS
    from defaults import NO_REP_MSG, NO_REQ_MSG
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR

class SRPCClient:
    def __init__(self, broker_addr:str = None, proxy_pub_addr:str = None, timeo:int = 1, last_msg_only:bool = True, no_rep_msg:str = None, no_req_msg:str = None):
        
        self._broker_addr = broker_addr if broker_addr else BROKER_ADDR
        self._proxy_pub_addr = proxy_pub_addr if proxy_pub_addr else PROXY_PUB_ADDR
        self._timeo = timeo
        self._last_msg_only = last_msg_only
        self.no_rep_msg = no_rep_msg if no_rep_msg else NO_REP_MSG
        self.no_req_msg = no_req_msg if no_req_msg else NO_REQ_MSG
        
        # it is better that the clients create their own, non shared context, in order to be able to use
        # clients inside other SRPCServer's
        self.ctx = zmq.Context()
        
        self._broker_client = ZMQServiceBrokerClient(self.ctx)
        self._broker_client.connect(self._broker_addr)
        
        self._proxy_sub = ZMQSub(ctx = self.ctx, last_msg_only = self._last_msg_only, timeo = self._timeo)
        self._proxy_sub.connect(self._proxy_pub_addr)

    def close(self):
        # if the client (or some class that inherits from SRPCClient) 
        # is being used where another shared context exits, then we must take
        # case not to terminate the context here as it may break the code 
        # when we try to close it!
        self._broker_client.close()
        self._proxy_sub.close()
        self.ctx.term()

    def subscribe(self, topic:str):
        self._proxy_sub.subscribe(topic)

    def listen(self):
        topic, msg = self._proxy_sub.recv()
        return topic, msg

    # must be subscribed before
    # waits for a topic (could just increase the timeout)
    def wait(self):
        while True:
            topic, msg = self.listen()
            if topic is not None:
                return topic, msg

    def call(self, service, method, args = [], kwargs = {}, close:bool = False):
        req = {
                "method": method,
                "args": args,
                "kwargs": kwargs
            }
        req = json.dumps(req)
        # Send the request
        status = self._broker_client.req(service = service, msg = req, timeo = self._timeo)
        if status == 1:
            rep = self._broker_client.rep(timeo = self._timeo)
            
            # print(rep)
            if rep is not None:                
                if rep != "ERROR":
                    rep = json.loads(rep)
                else:
                    rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)    
            else:
                # this should emulate the response from the server
                rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
        else:
            # this should emulate the response from the server
            rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_req_msg)
        if close: self.close()
        return rep

    # implement a method to parse the responses from the server
    # this seems to be practical
    def parse(self, rep):
        if rep.get('status') == OK_STATUS:
            return rep.get('output')
        else:
            return rep.get('error_msg')

    def invoque(self, service, method, args = [], kwargs = {}, close = False):
        return self.parse(self.call(service, method, args, kwargs, close))

def test_client():
    client = SRPCClient()
    print(client.invoque("test_server","add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
    print(client.invoque("test_server","subtract", [10, 43]))  # Output: {'result': -33}
    print(client.invoque("test_server","multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
    print(client.invoque("test_server","ExampleClass.multiply", [3, 4]))  # Output: {'result': 12}
    print(client.invoque("test_server","Store.set", ["ola", 1]))  # Output: {'result': 12}
    print(client.invoque("test_server","Store.get", ["ola"]))  # Output: {'result': 12}

if __name__ == "__main__":

    test_client()

