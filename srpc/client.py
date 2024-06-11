import zmq
import json

try:
    from .wrappers import SocketReqRep, SocketPub, SocketSub
except ImportError:
    from wrappers import SocketReqRep, SocketPub, SocketSub

class SRPCClient:
    def __init__(self, host:str, port:int, sub_port:int = None, recvtimeo:int = 1000, sndtimeo:int = 100, reconnect:int = 60*60, last_msg_only:bool = True):
        self.host = host
        self.port = port
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
        self.sub_socket = SocketSub(host = host, port = sub_port, recvtimeo = recvtimeo, last_msg_only = last_msg_only)

    def close(self):
        self.socket.close()
        self.sub_socket.close()

    def subscribe(self, key:str):
        self.sub_socket.subscribe(key)

    def listen(self):
        key, msg = self.sub_socket.recv()
        return key, msg

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
                rep = {"status":"error", "msg":"could not get a response from server"}
        else:
            rep = {"status":"error", "msg":"could not send to server"}
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

