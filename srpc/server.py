import zmq
import json
import time
import threading
import random
import string

try:
    from .wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic, clear_screen, QueueWrapper, Proxy
except ImportError:
    from wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic, clear_screen, QueueWrapper, Proxy
try:
    from .defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, PUBLISH_PERIOD
except ImportError:
    from defaults import REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, PUBLISH_PERIOD

# function to build a response from the server
# easier to check how the server responds

OK_STATUS = 'ok'
ERROR_STATUS = 'error'


def generate_random_string(n):
    # Define the character set: lowercase, uppercase letters, digits
    characters = string.ascii_letters + string.digits
    # Generate a random string
    random_string = ''.join(random.choice(characters) for _ in range(n))
    return random_string

def build_server_response(status:str, output, error_msg:str):
    return {'status':status, 'output':output, 'error_msg':error_msg}

class SRPCServer:
    def __init__(
                self, 
                name:str, 
                host:str, 
                port:int, 
                pub_port:int = None, 
                recvtimeo:int = 1000, 
                sndtimeo:int = 1000, 
                reconnect:int = 60*60, 
                registry_host:str = REGISTRY_HOST, 
                registry_port:int = REGISTRY_PORT, 
                queue_size:int = 2048, 
                n_workers:int = 1, 
                thread_safe:bool = False, 
                clear_screen:bool = True,
                worker_info:bool = False
                ):
        self.name = name
        self.srpc_host = host
        self.srpc_port = port
        self.srpc_pub_port = pub_port if pub_port is not None else port+1
        self.srpc_recvtimeo = recvtimeo
        self.srpc_sndtimeo = sndtimeo
        self.srpc_reconnect = reconnect

        self.worker_info = worker_info
        self.clear_screen = clear_screen
        self.socket = None        
        
        self.pub_socket = SocketPub(host = self.srpc_host, port = self.srpc_pub_port)

        self.registry_socket = SocketReqRep(
                                    host = registry_host, 
                                    port = registry_port, 
                                    zmq_type = 'REQ', 
                                    bind = False, 
                                    recvtimeo = 1000, 
                                    sndtimeo = 1000, 
                                    reconnect = reconnect
                                    )   

        self.functions = {}
        self.classes = {}
        self.class_instances = {}        
        self.last_heartbeat = time.time()
        self.stop_event = threading.Event() 
        self.pub_queue = QueueWrapper(4*4096)
        self.publish_lock = threading.Lock()
        self.stop_sockets_lock = threading.Lock()
        self.thread_safe_lock = threading.Lock()
        self.workers_lock = threading.Lock()
        self.thread_safe = thread_safe
        # build workers addr
        self.worker_addr = "inproc://"+generate_random_string(8)
        self.n_workers = n_workers

    def register_function(self, func, name=None):
        if name is None:
            name = func.__name__
        self.functions[name] = func

    def register_class(self, cls, name=None):
        if name is None:
            name = cls.__name__
        self.classes[name] = cls
        self.class_instances[name] = cls()

    def publish(self, topic:SRPCTopic, value:str):
        with self.publish_lock:
            if not self.stop_event.isSet(): 
                s = self.pub_queue.put([topic, value])
                if s == 1: print('Warning: pub queue is full.')
                return 1
            else:
                return 0

    def handle_request(self, request):
        method = request.get("method")
        args = request.get("args", []) 
        kwargs = request.get("kwargs", {}) 
        
        if hasattr(self, method):
            try:
                method = getattr(self, method)
                out = method(*args, **kwargs)
                rep = build_server_response(status = OK_STATUS, output = out, error_msg = '')

            except Exception as e:
                rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))
                
        elif method in self.functions:
            try:
                out = self.functions[method](*args, **kwargs)
                rep = build_server_response(status = OK_STATUS, output = out, error_msg = '')
            except Exception as e:
                rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))
        else:
            rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = f"Unknown method: {method}")
            if "." in method:
                class_name, method_name = method.split('.', 1)
                if class_name in self.class_instances and hasattr(self.class_instances[class_name], method_name):
                    try:
                        method = getattr(self.class_instances[class_name], method_name)
                        out = method(*args, **kwargs)
                        rep = build_server_response(status = OK_STATUS, output = out, error_msg = '')
                    except Exception as e:
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))      
        return rep

    def srpc_close(self):
        # self.socket.close()
        self.registry_socket.close()
        self.pub_socket.close()
        self.pub_queue.close() 
        self.pub_queue = None
        print(f"Server {self.name} closed")


    def registry_heartbeat(self):
        while not self.stop_event.isSet(): 
            try:
                req = {
                    "action": "heartbeat",
                    "info": {
                        "name": self.name,
                        "req_address": f"tcp://{self.srpc_host}:{self.srpc_port}",
                        "pub_address": f"tcp://{self.srpc_host}:{self.srpc_pub_port}"
                    }
                }
                status = self.registry_socket.send(json.dumps(req))
                if status == 1:
                    rep = self.registry_socket.recv()
                # make it exit faster
                s = time.time()
                while time.time() - s < REGISTRY_HEARTBEAT:
                    time.sleep(0.5)
                    if self.stop_event.isSet():
                        return
            except:
                print('Error in registry_heartbeat')
                pass

    def publisher(self):
        while not self.stop_event.isSet(): 
            tmp = self.pub_queue.get(PUBLISH_PERIOD)
            if tmp is not None:
                self.pub_socket.publish(tmp[0], tmp[1])

    def base_worker(self):
        # base worker should create its socket

        worker_name = "worker_" + generate_random_string(5)

        worker_socket = SocketReqRep(
                                    zmq_type = 'REP', 
                                    bind = False, 
                                    recvtimeo = self.srpc_recvtimeo, 
                                    sndtimeo = self.srpc_sndtimeo, 
                                    reconnect = self.srpc_reconnect,
                                    addr = self.worker_addr,
                                    shared_context = True
                                    )
        
        while not self.stop_event.isSet():            
            try:
                req = worker_socket.recv()
                if req is not None:            
                    if self.worker_info: print('Request on ', worker_name)
                    try:
                        # req to json
                        req = json.loads(req)                        
                        if self.thread_safe:
                            with self.thread_safe_lock:
                                rep = self.handle_request(req)    
                        else:
                            rep = self.handle_request(req)
                        rep = json.dumps(rep)
                        worker_socket.send(rep)
                    except json.JSONDecodeError:
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = 'Invalid json')     
                        worker_socket.send(rep)
                    except Exception as e:
                        print('SRPCServer error: ', e)
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))   
                        worker_socket.send(rep)
            except Exception as e:
                print('Error in base_worker: ', e)
                break        
        # do not terminate the context as it is shared
        worker_socket.close(False)

    @property
    def client_addr(self):
        addr = f"tcp://{self.srpc_host}:{self.srpc_port}"    
        addr = addr.replace('localhost','127.0.0.1')
        return addr
    
    def srpc_serve(self):
        if self.clear_screen: 
            clear_screen()
            self.clear_screen = False

        print(f"Server {self.name} running")
        
        # start registry communications
        reg_th = threading.Thread(target = self.registry_heartbeat, daemon = True)
        reg_th.start()
        
        # start publisher
        pub_th = threading.Thread(target = self.publisher, daemon = True)
        pub_th.start()                     
        
        # start proxy
        proxy = Proxy(self.worker_addr, self.client_addr)
        proxy.start()         

        # start workers

        workers = []
        for _ in range(self.n_workers): # Number of worker threads
            thread = threading.Thread(target=self.base_worker)
            thread.start()
            workers.append(thread)

        while True:
            try:
                time.sleep(0.1)
                pass
            except KeyboardInterrupt:
                break
        self.stop_event.set()
        print(f'Server {self.name} stopping proxy')                
        proxy.stop()
        print(f'Server {self.name} joining workers')
        for worker in workers:
            worker.join()
        print(f'Server {self.name} joining publisher')
        pub_th.join()
        print(f'Server {self.name} joining registry')
        reg_th.join()
        self.close()

    # call start and then serve
    # make it easier to dev services
    def serve(self):
        if self.clear_screen: 
            clear_screen()
            self.clear_screen = False        
        if hasattr(self, 'start'):
            print(f"Server {self.name} initializing")
            self.start()
        self.srpc_serve()

    # override this if you want
    def close(self):
        self.srpc_close()

    def start(self):
        '''
        override this method
        '''
        pass


def test_server():
    def add(a, b):
        return a + b

    def subtract(a, b):
        return a - b

    class ExampleClass:
        def multiply(self, a, b):
            return a * b

        def divide(self, a, b):
            if b == 0:
                return "Cannot divide by zero"
            return a / b

    server = SRPCServer(name = 'test_server', host = "localhost", port = 5557, clear_screen = False, n_workers = 5, thread_safe = True)
    server.register_function(add)
    server.register_function(subtract)
    server.register_class(ExampleClass)  

    server.serve()




if __name__ == "__main__":

    test_server()

