import zmq
import json

try:
    from .wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic
    from .defaults import NO_REP_MSG, NO_REQ_MSG
except ImportError:
    from wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic
    from defaults import NO_REP_MSG, NO_REQ_MSG


class SRPCClient:
    def __init__(self, host:str, port:int, sub_port:int = None, recvtimeo:int = 1000, sub_recvtimeo:int = 1000, sndtimeo:int = 100, reconnect:int = 60*60, last_msg_only:bool = True, no_rep_msg = NO_REP_MSG, no_req_msg = NO_REQ_MSG):
        self.host = host
        self.port = port
        self.no_rep_msg = no_rep_msg
        self.no_req_msg = no_req_msg
        sub_port = sub_port if sub_port is not None else port+1
        self.socket = SocketReqRep(
                                    host = host, 
                                    port = port, 
                                    zmq_type = 'REQ', 
                                    bind = False, 
                                    recvtimeo = recvtimeo, 
                                    sndtimeo = sndtimeo, 
                                    reconnect = reconnect
                                    )
        self.sub_socket = SocketSub(host = host, port = sub_port, recvtimeo = sub_recvtimeo, last_msg_only = last_msg_only)

    def close(self):
        self.socket.close()
        self.sub_socket.close()

    def subscribe(self, topic:SRPCTopic, unsubscribe:bool = True):
        self.sub_socket.subscribe(topic, unsubscribe)

    def listen(self):
        topic, msg = self.sub_socket.recv()
        return topic, msg

    def call(self, method, args = [], kwargs = {}, close = False):
        req = {
                "method": method,
                "args": args,
                "kwargs": kwargs
            }
        req = json.dumps(req)
        # Send the request
        status = self.socket.send(req)
        if status == 1:
            rep = self.socket.recv()
            if rep is not None:                
                rep = json.loads(rep)
            else:
                rep = {"status":"error", "msg":self.no_rep_msg}
        else:
            rep = {"status":"error", "msg":self.no_req_msg}
        if close: self.close()
        return rep

if __name__ == "__main__":

    client = SRPCClient(host = "localhost", port = 5557)
    
    print(client.call("add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
    print(client.call("subtract", [10, 43]))  # Output: {'result': 6}
    print(client.call("multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
    print(client.call("ExampleClass.multiply", [3, 4]))  # Output: {'result': 12}
    print(client.call("Store.set", ["ola", 1]))  # Output: {'result': 12}
    print(client.call("Store.get", ["ola"]))  # Output: {'result': 12}

