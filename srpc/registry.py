import zmq
import json
import threading
import time
import datetime as dt
import os
try:
    from .utils import clear_screen
    from .custom_zmq import ZMQR, ZMQP
except ImportError:
    from utils import clear_screen
    from custom_zmq import ZMQR, ZMQP
try:
    from .defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT
except ImportError:
    from defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT


class SRPCRegistry:
    def __init__(self, addr:str = None, timeo:int = 10):
        self.addr = addr if addr else REGISTRY_ADDR
        self.ctx = zmq.Context()
        self.socket = ZMQR(ctx = self.ctx, zmq_type = zmq.REP, timeo = timeo)
        self.socket.bind(self.addr)
        self.services = {}
    
    def close(self):
        self.socket.close()      
        self.ctx.term()
        print("Registry closed")  

    def handle_heartbeat(self, info = {}):
        self.services[info["name"]] = {
                                        "rep_address": info.get("rep_address",'unk'),
                                        "pub_address": info.get("pub_address",'unk'),
                                        "last_heartbeat": time.time(),
                                        "ts": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        }
        return {"status": "ok"}

    def list_services(self):
        services_list = {name: info["address"] for name, info in self.services.items()}
        return {"status":"ok", "services": services_list}

    def serve(self):
        while True:
            try:
                # show info
                clear_screen()
                
                print(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] SRPC REGISTRY on {self.addr} ")
                print()
                for name, info in self.services.items():
                    print(f">> SERVICE {name} | ACCEPT REQ ON {info.get('rep_address')} | PUB ON {info.get('pub_address')} | LAST INFO AT [{info.get('ts')}]")

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
                        print('rep: ', rep)
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
    registry.serve()
    