# example of a remote dict service with the framework

import threading
import pickle
import os
import datetime as dt
import time
try:
    from .server import SRPCServer, rpc_method
    from .utils import SRPCTopic   
    from .client import SRPCClient
    from .defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

except ImportError:
    from server import SRPCServer, rpc_method
    from utils import SRPCTopic
    from client import SRPCClient
    from defaults import REGISTRY_ADDR, REGISTRY_HEARTBEAT, NO_REP_MSG, NO_REQ_MSG

try:
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR



class Store(SRPCServer):
    def __init__(
        self,
        broker_addr=None,
        proxy_sub_addr=None,
        registry_addr=None,
        service_name="Store",
        filename="store.pkl",
        backup_interval_minutes=5,
    ):
        super().__init__(
            name=service_name,
            broker_addr=broker_addr,
            proxy_sub_addr=proxy_sub_addr,
            timeo=1,
            n_workers=1,
            thread_safe=False,
            clear_screen=True,
        )

        self.filename = filename
        self.store = {}
        self.lock = threading.RLock()
        self.backup_interval_seconds = backup_interval_minutes * 60
        self.last_save_time = 0.0
        self._store_saved_on_close = False
        self.read_store()

    @rpc_method
    def set_default_store(self):
        with self.lock:
            self.store = {"pub_msgs": {}}
            self._maybe_write_store()

    def read_store(self):
        with self.lock:
            if not os.path.exists(self.filename):
                self.store = {}
                self.last_save_time = time.time()
                return

            try:
                with open(self.filename, "rb") as file:
                    data = pickle.load(file)
                    self.store = data if isinstance(data, dict) else {}
            except Exception:
                self.store = {}

            self.last_save_time = time.time()

    def write_store(self):
        with self.lock:
            tmpfile = f"{self.filename}.tmp"
            with open(tmpfile, "wb") as file:
                pickle.dump(self.store, file, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmpfile, self.filename)
            self.last_save_time = time.time()

    def _maybe_write_store(self):
        with self.lock:
            now = time.time()
            if now - self.last_save_time >= self.backup_interval_seconds:
                self.write_store()

    def close(self):
        self._close()
        if self._closed:
            with self.lock:
                if not self._store_saved_on_close:
                    self.write_store()
                    self._store_saved_on_close = True

    @rpc_method
    def clear(self):
        with self.lock:
            self.store = {}
            self._maybe_write_store()

    @rpc_method
    def keys(self):
        with self.lock:
            return list(self.store.keys())

    def _walk_to_parent(self, keys, create=False):
        if not keys:
            raise ValueError("at least one key is required")

        d = self.store
        for key in keys[:-1]:
            if key not in d:
                if not create:
                    return None, keys[-1]
                d[key] = {}
            elif not isinstance(d[key], dict):
                raise TypeError(f"Intermediate key {key!r} is not a dict")
            d = d[key]

        return d, keys[-1]

    @rpc_method
    def set(self, *args):
        if len(args) < 2:
            raise ValueError("set requires at least one key and one value")

        *keys, value = args

        with self.lock:
            d, last_key = self._walk_to_parent(keys, create=True)
            d[last_key] = value
            self._maybe_write_store()

    @rpc_method
    def setappend(self, *args):
        '''
        '''
        if len(args) < 2:
            raise ValueError("set requires at least one key and one value")

        *keys, value = args

        with self.lock:
            d, last_key = self._walk_to_parent(keys, create=True)

            if last_key not in d:
                d[last_key] = value
            else:
                existing = d[last_key]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    d[last_key] = [existing, value]

            self._maybe_write_store()

    @rpc_method
    def get(self, *keys, default=None):
        with self.lock:
            if not keys:
                return self.store

            cur = self.store
            for key in keys:
                if not isinstance(cur, dict):
                    return default
                if key not in cur:
                    return default
                cur = cur[key]
            return cur

    @rpc_method
    def sget(self, key, sub_key=None):
        with self.lock:
            value = self.store.get(key)
            if sub_key is None:
                return value
            if isinstance(value, dict):
                return value.get(sub_key)
            return None

    @rpc_method
    def delete(self, *keys):
        if not keys:
            raise ValueError("delete requires at least one key")

        with self.lock:
            if len(keys) == 1:
                self.store.pop(keys[0], None)
            else:
                d, last_key = self._walk_to_parent(keys, create=False)
                if d is not None and isinstance(d, dict):
                    d.pop(last_key, None)

            self._maybe_write_store()


class StoreClient(SRPCClient):
    def __init__(self, srpc_client:SRPCClient, service_name:str = 'Store', timeo:int = 1, last_msg_only:bool = True, no_rep_msg:str = None, no_req_msg:str = None):        
        self.service_name = service_name
        self.srpc_client = srpc_client
        if no_rep_msg: self.srpc_client.no_rep_msg = no_rep_msg
        if no_req_msg: self.srpc_client.no_req_msg = no_req_msg

    def clear(self):
        return self.srpc_client.invoque(service = self.service_name, method = 'clear', args = [], kwargs = {}, close = False)

    def delete(self, *keys):
        return self.srpc_client.invoque(service = self.service_name, method = 'delete', args = keys, kwargs = {}, close = False)

    def keys(self):
        return self.srpc_client.invoque(service = self.service_name, method = 'keys', args = [], kwargs = {}, close = False)

    def sget(self, key, sub_key = None):
        return self.srpc_client.invoque(service = self.service_name, method = 'sget', args = [], kwargs = {'key':key, 'sub_key': sub_key}, close = False)

    def get(self, *keys):
        return self.srpc_client.invoque(service = self.service_name, method = 'get', args = keys, kwargs = {}, close = False)
        
    def set(self, *keys):
        return self.srpc_client.invoque(service = self.service_name, method = 'set', args = keys, kwargs = {}, close = False)

    def setappend(self, *keys):
        return self.srpc_client.invoque(service = self.service_name, method = 'setappend', args = keys, kwargs = {}, close = False)        

    def publish(self, topic:str, msg:str):
        return self.srpc_client.invoque(service = self.service_name, method = 'publish', args = [], kwargs = {'topic':topic, 'msg':msg}, close = False)

        
def test_server():
    server = Store(service_name = 'Store')  
    server.serve()


def test_client():
   
    srpc_client = SRPCClient()

    client = StoreClient(srpc_client)
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

    srpc_client.close()
   

if __name__ == "__main__":
    # test_server()
    test_client()
