# example of a remote dict service with the framework

import threading
import pickle
import os

try:
    from .server import SRPCServer
    from .client import SRPCClient
    from .defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

except ImportError:
    from server import SRPCServer
    from client import SRPCClient
    from defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG


class Store(SRPCServer):
    def __init__(self, service_name:str, host:str, port:int, pub_port:int = None, registry_host:str = REGISTRY_HOST, registry_port:int = REGISTRY_PORT, filename:str = 'store.pkl'):
        SRPCServer.__init__(self, name = service_name, host = host, port = port, pub_port = pub_port, registry_host = registry_host, registry_port = registry_port)
        self.filename = filename
        self.store = {}
        self.locks = {}
        self.global_lock = threading.Lock()
        # read store from disk at close
        self.read_store()

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
        self.srpc_close()


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
        #if key in d:
        #    return recursive_get(d[key], keys[1:])
        #else:
        #    raise KeyError(f"Key '{key}' not found in dictionary")


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
    def __init__(self, host, port, sub_port = None, recvtimeo:int = 1000, sub_recvtimeo:int = 1000, sndtimeo:int = 100, no_rep_msg = NO_REP_MSG, no_req_msg = NO_REQ_MSG):
        SRPCClient.__init__(self, host = host, port = port, sub_port = sub_port, recvtimeo = 1000, sub_recvtimeo = 1000, sndtimeo = 100, no_rep_msg = no_rep_msg, no_req_msg = no_req_msg)

    #def parse(self, rep):
    #    if rep.get('status') == "ok":
    #        return rep.get('value')
    #    else:
    #        return rep.get('msg')

    def clear(self):
        rep = self.call(method = 'clear', args = [], kwargs = {}, close = False)
        return self.parse(rep)

    def delete(self, key):
        rep = self.call(method = 'delete', args = [], kwargs = {'key':key}, close = False)
        return self.parse(rep)

    def keys(self):
        rep = self.call(method = 'keys', args = [], kwargs = {}, close = False)
        return self.parse(rep)

    def sget(self, key, sub_key = None):
        rep = self.call(method = 'sget', args = [], kwargs = {'key':key, 'sub_key': sub_key}, close = False)
        return self.parse(rep)

    def get(self, *keys):
        rep = self.call(method = 'get', args = keys, kwargs = {}, close = False)
        return self.parse(rep)        

    def set(self, key, value):
        rep = self.call(method = 'set', args = [], kwargs = {'key':key, 'value':value}, close = False)
        return self.parse(rep)

def store_service(host, port, name = 'store'):
    server = Store(service_name = 'store', host = host, port = port)  
    server.serve()

def test_client():
   
    client = StoreClient("localhost", 5550)
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
    # store_service("localhost", 5550)
    test_client()
