# example of a remote dict service with the framework

import threading

try:
    from .server import SRPCServer
    from .client import SRPCClient
except ImportError:
    from server import SRPCServer
    from client import SRPCClient


class Store(SRPCServer):
    def __init__(self, service_name, host, port):
        SRPCServer.__init__(self, name = service_name, host = host, port = port)
        self.store = {}
        self.locks = {}
        self.global_lock = threading.Lock()

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

    def get(self, key):
        lock = self._get_lock(key)
        with lock:
            return self.store.get(key)

    def delete(self, key):
        lock = self._get_lock(key)
        with lock:
            if key in self.store:
                del self.store[key]

class StoreClient(SRPCClient):
    def __init__(self, host, port):
        SRPCClient.__init__(self, host = host, port = port)

    def parse(self, rep):
        if rep.get('status') == "ok":
            return rep.get('value')
        else:
            return rep.get('msg')

    def delete(self, key):
        rep = self.call(method = 'delete', args = [], kwargs = {'key':key}, close = False)
        return self.parse(rep)

    def keys(self):
        rep = self.call(method = 'keys', args = [], kwargs = {}, close = False)
        return self.parse(rep)

    def get(self, key):
        rep = self.call(method = 'get', args = [], kwargs = {'key':key}, close = False)
        return self.parse(rep)

    def set(self, key, value):
        return self.call(method = 'set', args = [], kwargs = {'key':key, 'value':value}, close = False)
        return self.parse(rep)

def store_service(host, port):
    server = Store(service_name = 'store', host = host, port = port)  
    server.srpc_serve()

def test_client():
   
    client = StoreClient("localhost", 5550)
    print(client.set('ola',1))
    print(client.get('ola'))
    print(client.get('ole'))
    print(client.keys())
    print(client.delete('ola'))
    print(client.keys())
    client.close()
   

if __name__ == "__main__":
    # store_service("localhost", 5550)
    test_client()
