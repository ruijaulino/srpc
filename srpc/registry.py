import zmq
import json
import threading
import time
import datetime as dt
import os
try:
    from .wrappers import SocketReqRep, SocketPub, SocketSub, clear_screen
except ImportError:
    from wrappers import SocketReqRep, SocketPub, SocketSub, clear_screen
try:
    from .defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT
except ImportError:
    from defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT


class SRPCRegistry:
    def __init__(self, host:str = REGISTRY_HOST, port:int = REGISTRY_PORT, recvtimeo:int = 10000, sndtimeo:int = 100, reconnect:int = 60*60):
        self.host = host
        self.port = port
        self.socket = SocketReqRep(
                                    host = host, 
                                    port = port, 
                                    zmq_type = 'REP', 
                                    bind = True, 
                                    recvtimeo = recvtimeo, 
                                    sndtimeo = sndtimeo, 
                                    reconnect = reconnect
                                    )
        self.services = {}
    
    def close(self):
        self.socket.close()      
        print("Registry closed")  

    def handle_heartbeat(self, info = {}):
        self.services[info["name"]] = {
                                        "address": info["address"],
                                        "last_heartbeat": time.time(),
                                        "ts": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        }
        return {"status": "ok"}

    def list_services(self):
        services_list = {name: info["address"] for name, info in self.services.items()}
        return {"status":"ok", "services": services_list}

    def srpc_serve(self):
        while True:
            try:
                # show info
                clear_screen()
                print(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] SRPC REGISTRY on {self.host}:{self.port} ")
                print()
                for name, info in self.services.items():
                    print(f">> SERVICE {name} RUNNING ON {info.get('address')} | LAST INFO AT [{info.get('ts')}]")

                req = self.socket.recv()
                if req is not None:                
                    try:
                        # req to json
                        req = json.loads(req)
                        if req["action"] == "heartbeat":
                            rep = self.handle_heartbeat(req.get("info",{}))
                        elif request["action"] == "services":
                            rep = self.list_services()
                        else:
                            rep = {"status":"error", "msg": "unknown request type"}
                        rep = json.dumps(rep)
                        status = self.socket.send(rep)
                    except json.JSONDecodeError:
                        rep = json.dumps({'status': 'error', 'msg': 'Invalid json'})
                        self.socket.send(rep)
                    except Exception as e:
                        rep = json.dumps({'status': 'error', 'msg': e})
                        self.socket.send(rep)
                
                # check for dead services
                # this works because the recv is always asynch
                t = time.time()
                n_del = []
                for name, service in self.services.items():
                    if t - service["last_heartbeat"] > REGISTRY_HEARTBEAT*2: n_del.append(name)
                for n in n_del: del self.services[n]      
            except KeyboardInterrupt:
                break
        self.close()      

if __name__ == "__main__":
    registry = SRPCRegistry()
    registry.srpc_serve()
