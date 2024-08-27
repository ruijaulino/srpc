# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
try:
    from .server import SRPCServer
    from .utils import SRPCTopic   
    from .client import SRPCClient
    from .defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

except ImportError:
    from server import SRPCServer
    from utils import SRPCTopic
    from client import SRPCClient
    from defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG


class Store(SRPCServer):
    def __init__(self, rep_addr:str, pub_addr:str, registry_addr:str = None, service_name:str = "Store", filename:str = 'store.pkl'):
        
        SRPCServer.__init__(
                            self, 
                            name = service_name, 
                            rep_addr = rep_addr,
                            pub_addr = pub_addr,
                            registry_addr = registry_addr,
                            timeo = 1, 
                            n_workers = 1, 
                            thread_safe = False, 
                            lvc = True,
                            clear_screen = True
                            )

        self.filename = filename
        self.store = {}
        self.locks = {}
        self.global_lock = threading.Lock()
        # read store from disk at close
        self.read_store()

    def set_default_store(self):
        self.store = {'pub_msgs':{}}

    def read_store(self):
        """Reads a Python object from disk using pickle.
        
        Returns an empty dictionary if the file does not exist.
        """
        with self.global_lock:
            if not os.path.exists(self.filename):
                self.store = {}
            else:
                with open(self.filename, 'rb') as file:
                    self.store = pickle.load(file)

    def write_store(self):
        """Writes a Python object to disk using pickle."""
        with open(self.filename, 'wb') as file:
            pickle.dump(self.store, file, protocol=pickle.HIGHEST_PROTOCOL)

    def close(self):
        # write store to disk at close
        self.write_store()
        self._close()

    def _get_lock(self, key):
        with self.global_lock:
            if key not in self.locks:
                self.locks[key] = threading.Lock()
            return self.locks[key]

    def clear(self):
        with self.global_lock:
            self.store = {}

    def keys(self):
        with self.global_lock:
            return list(self.store.keys())        

    def set(self, key, value):
        lock = self._get_lock(key)
        with lock:
            self.store[key] = value

    @staticmethod
    def recursive_get(d, keys):
        if not keys:
            return d
        if not isinstance(d, dict): return None
        key = keys[0]
        return Store.recursive_get(d.get(key, None), keys[1:])

    def get(self, *keys):
        with self.global_lock:
            if not keys: return None 
        lock = self._get_lock(keys[0])
        with lock:
            return Store.recursive_get(self.store, keys)


    def sget(self, key, sub_key = None):
        lock = self._get_lock(key)
        with lock:
            if sub_key is not None:
                try:
                    return self.store.get(key,{}).get(sub_key)
                except:
                    return None
            else:
                return self.store.get(key)

    def delete(self, key):
        lock = self._get_lock(key)
        with lock:
            if key in self.store:
                del self.store[key]
    
class StoreClient(SRPCClient):
    def __init__(self, req_addr:str, sub_addr:str = None, timeo:int = 1, last_msg_only:bool = True, no_rep_msg = NO_REP_MSG, no_req_msg = NO_REQ_MSG):        
        SRPCClient.__init__(self, req_addr = req_addr, sub_addr = sub_addr, timeo = timeo, last_msg_only = last_msg_only, no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    def clear(self):
        return self.invoque(method = 'clear', args = [], kwargs = {}, close = False)

    def delete(self, key):
        return self.invoque(method = 'delete', args = [], kwargs = {'key':key}, close = False)

    def keys(self):
        return self.invoque(method = 'keys', args = [], kwargs = {}, close = False)

    def sget(self, key, sub_key = None):
        return self.invoque(method = 'sget', args = [], kwargs = {'key':key, 'sub_key': sub_key}, close = False)

    def get(self, *keys):
        return self.invoque(method = 'get', args = keys, kwargs = {}, close = False)
        
    def set(self, key, value):
        return self.invoque(method = 'set', args = [], kwargs = {'key':key, 'value':value}, close = False)

    def publish(self, topic:str, msg:str):
        return self.invoque(method = 'publish', args = [], kwargs = {'topic':topic, 'msg':msg}, close = False)
        
def test_server():
    server = Store(service_name = 'store', rep_addr = "tcp://127.0.0.1:5551", pub_addr = "tcp://127.0.0.1:5552", registry_addr = REGISTRY_ADDR)  
    server.serve()

def test_client():
   
    client = StoreClient(req_addr = "tcp://127.0.0.1:5551", sub_addr = "tcp://127.0.0.1:5552")
    print(client.clear())
    print(client.set('ola',1))
    print(client.get('ola'))
    print(client.get('ole'))
    print(client.keys())
    print(client.delete('ola'))
    print(client.keys())
    print('multilevel..')
    print(client.set('d0',{'d1':{'d11':1,'d12':[1,2,3]}}))
    print(client.get('d0'))
    print()
    print(client.get('d0','d1'))
    print()
    print(client.get('d0','b','d12'))
    print()

    client.close()
   

if __name__ == "__main__":
    test_server()
    # test_client()
