import zmq
import json
import time

try:
    from .custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker, ZMQS
    from .utils import build_server_response, OK_STATUS, ERROR_STATUS
    from .defaults import NO_REP_MSG, NO_REQ_MSG
except ImportError:
    from custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker, ZMQS
    from utils import build_server_response, OK_STATUS, ERROR_STATUS
    from defaults import NO_REP_MSG, NO_REQ_MSG


class SRPCClient:
    def __init__(self, req_addr:str, sub_addr:str = False, timeo:int = 1, last_msg_only:bool = True, no_rep_msg:str = None, no_req_msg:str = None):
        self._req_addr = req_addr
        self._sub_addr = sub_addr
        self._timeo = timeo
        self._last_msg_only = last_msg_only
        self._no_rep_msg = no_rep_msg if no_rep_msg else NO_REP_MSG
        self._no_req_msg = no_req_msg if no_req_msg else NO_REQ_MSG
        
        self.ctx = zmq.Context.instance()
        
        self.req_socket = ZMQR(ctx = self.ctx, zmq_type = zmq.REQ, timeo = self._timeo)
        self.req_socket.connect(self._req_addr)
        
        self.sub_socket = None
        if self._sub_addr:
            self.sub_socket = ZMQS(ctx = self.ctx, last_msg_only = self._last_msg_only, timeo = self._timeo)
            self.sub_socket.connect(self._sub_addr)

    def close(self, term = True):
        # if the client (or some class that inherits from SRPCClient) 
        # is being used where another shared context exits, then we must take
        # case not to terminate the context here as it may break the code 
        # when we try to close it!
        self.req_socket.close()
        if self.sub_socket: self.sub_socket.close()
        if term: self.ctx.term()

    def subscribe(self, topic:str):
        if self.sub_socket:
            self.sub_socket.subscribe(topic)
        else:
            print('Trying to subscribe on a client without a defined subscriber')
            return None, None

    def listen(self):
        if self.sub_socket:
            topic, msg = self.sub_socket.recv()
            return topic, msg
        else:
            print('Trying to listen on a client without a defined subscriber')
            return None, None

    def call(self, method, args = [], kwargs = {}, close = False):
        req = {
                "method": method,
                "args": args,
                "kwargs": kwargs
            }
        req = json.dumps(req)
        # Send the request
        status = self.req_socket.send(req)
        if status == 1:
            rep = self.req_socket.recv()
            if rep is not None:                
                rep = json.loads(rep)
            else:
                # this should emulate the response from the server
                rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self._no_rep_msg)
        else:
            # this should emulate the response from the server
            rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self._no_req_msg)
        if close: self.close()
        return rep

    # implement a method to parse the responses from the server
    # this seems to be practical
    def parse(self, rep):
        if rep.get('status') == OK_STATUS:
            return rep.get('output')
        else:
            return rep.get('error_msg')

    def invoque(self, method, args = [], kwargs = {}, close = False):
        return self.parse(self.call(method, args, kwargs, close))

def test_client():
    client = SRPCClient(req_addr = "tcp://127.0.0.1:5557", sub_addr = "tcp://127.0.0.1:5558")
    
    print(client.call("add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
    print(client.call("subtract", [10, 43]))  # Output: {'result': -33}
    print(client.call("multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
    print(client.call("ExampleClass.multiply", [3, 4]))  # Output: {'result': 12}
    print(client.call("Store.set", ["ola", 1]))  # Output: {'result': 12}
    print(client.call("Store.get", ["ola"]))  # Output: {'result': 12}

if __name__ == "__main__":

    test_client()

