import zmq
import json
import time
import threading
import random
import string

try:
    from .wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic, clear_screen, QueueWrapper, Proxy
    from .custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker
except ImportError:
    from wrappers import SocketReqRep, SocketPub, SocketSub, SRPCTopic, clear_screen, QueueWrapper, Proxy
    from custom_zmq import ZMQR, ZMQP, ZMQReliableQueue, ZMQReliableQueueWorker
try:
    from .defaults import REGISTRY_ADDR, REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, PUBLISH_PERIOD
except ImportError:
    from defaults import REGISTRY_ADDR, REGISTRY_HOST, REGISTRY_PORT, REGISTRY_HEARTBEAT, PUBLISH_PERIOD

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
                rep_addr:str,
                pub_addr:str,
                registry_addr:str = REGISTRY_ADDR,
                timeo:int = 1, 
                n_workers:int = 1, 
                thread_safe:bool = False, 
                lvc:bool = True,
                clear_screen:bool = True,
                ):
        self._name = name
        self._rep_addr = rep_addr
        self._pub_addr = pub_addr
        self._registry_addr = registry_addr
        self._lvc = lvc
        self._timeo = timeo
        self._clear_screen = clear_screen

        # create zmq context        
        self.ctx = zmq.Context.instance()
        self.stop_event = threading.Event()  

        self._pub_socket = ZMQP(ctx = self.ctx, lvc = self._lvc, timeo = self._timeo)
        self._pub_socket.bind(self._pub_addr)

        self._registry_socket = ZMQR(ctx = self.ctx, zmq_type = zmq.REQ, timeo = self._timeo)
        self._registry_socket.connect(self._registry_addr)

        self._functions = {}
        self._classes = {}
        self._class_instances = {}        
        
        self._thread_safe_lock = threading.Lock()
        self._thread_safe = thread_safe
        self._n_workers = n_workers

        self._workers = []
        self._reg_th = None
        self._req_queue = None
        # build workers addr
        self._worker_addr = "inproc://"+generate_random_string(8)

    def registry_heartbeat(self):
        while not self.stop_event.isSet(): 
            try:
                req = {
                    "action": "heartbeat",
                    "info": {
                        "name": self._name,
                        "rep_address": self._rep_addr,
                        "pub_address": self._pub_addr
                    }
                }
                status = self._registry_socket.send(json.dumps(req))
                if status == 1:
                    rep = self._registry_socket.recv()
                # make it exit faster
                s = time.time()
                while time.time() - s < REGISTRY_HEARTBEAT:
                    time.sleep(0.5)
                    if self.stop_event.isSet():
                        return
            except Exception as e:
                print('Error in registry_heartbeat: ', e)
                pass

    def publish(self, topic:str, msg:str):
        if not isinstance(msg, str):
            try:
                msg = str(msg)
            except:
                print('could not cast msg to publish into string')    
                msg = None
        if msg: self._pub_socket.publish(topic = topic, msg = msg)

    def register_function(self, func, name=None):
        if name is None:
            name = func.__name__
        self._functions[name] = func

    def register_class(self, cls, name=None):
        if name is None:
            name = cls.__name__
        self._classes[name] = cls
        self._class_instances[name] = cls()

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
                
        elif method in self._functions:
            try:
                out = self._functions[method](*args, **kwargs)
                rep = build_server_response(status = OK_STATUS, output = out, error_msg = '')
            except Exception as e:
                rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))
        else:
            rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = f"Unknown method: {method}")
            if "." in method:
                class_name, method_name = method.split('.', 1)
                if class_name in self._class_instances and hasattr(self._class_instances[class_name], method_name):
                    try:
                        method = getattr(self._class_instances[class_name], method_name)
                        out = method(*args, **kwargs)
                        rep = build_server_response(status = OK_STATUS, output = out, error_msg = '')
                    except Exception as e:
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))      
        return rep


    def base_worker(self):
        # base worker should create its socket

        wid = "worker_" + generate_random_string(5)
        # print(f'Worker {wid} started')

        worker_socket = ZMQReliableQueueWorker(self.ctx)
        worker_socket.connect(self._worker_addr)
        
        while not self.stop_event.isSet():            
            try:
                clientid, req = worker_socket.recv_work()
                if req is not None:            
                    # if self.worker_info: print('Request on ', wid)
                    try:
                        # req to json
                        req = json.loads(req)                        
                        if self._thread_safe:
                            with self._thread_safe_lock:
                                rep = self.handle_request(req)    
                        else:
                            rep = self.handle_request(req)
                        rep = json.dumps(rep)
                        worker_socket.send_work(clientid, rep)
                    except json.JSONDecodeError:
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = 'Invalid json')     
                        worker_socket.send_work(clientid, rep)
                    except Exception as e:
                        print('SRPCServer error: ', e)
                        rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = str(e))   
                        worker_socket.send_work(clientid, rep)
            except Exception as e:
                print('Error in base_worker: ', e)
                break        
        # do not terminate the context as it is shared
        worker_socket.close()
        # print(f'Worker {wid} closed')
    
    def _serve(self):
        if self._clear_screen: 
            clear_screen()
            self._clear_screen = False

        print(f"Server {self._name} running")
        
        # start registry communications
        self._reg_th = threading.Thread(target = self.registry_heartbeat, daemon = True)
        self._reg_th.start()
                
        # start queue to distribute work
        self._req_queue = ZMQReliableQueue(ctx = self.ctx, client_addr = self._rep_addr, worker_addr = self._worker_addr)
        self._req_queue.start()
        # start workers
        self._workers = []
        for _ in range(self._n_workers): # Number of worker threads
            thread = threading.Thread(target=self.base_worker)
            thread.start()
            self._workers.append(thread)

        # keep server alive
        while True:
            try:
                time.sleep(0.1)
                pass
            except KeyboardInterrupt:
                break

        self.close()

    # call start and then serve
    # make it easier to dev services
    def serve(self):
        if self._clear_screen: 
            clear_screen()
            self._clear_screen = False        
        if hasattr(self, 'start'):
            print(f"Server {self._name} initializing")
            self.start()
        self._serve()

    def _close(self):
        self.stop_event.set()
        print(f'Server {self._name} joining workers')
        for worker in self._workers:
            worker.join()
        print(f'Server {self._name} stopping request queue')        
        self._req_queue.stop()
        print(f'Server {self._name} joining registry')
        self._reg_th.join()        
        self._registry_socket.close()
        self._pub_socket.close()
        self.ctx.term()
        print(f"Server {self._name} closed")


    # override this if you want
    def close(self):
        self._close()

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

    server = SRPCServer(name = 'test_server', rep_addr = "tcp://127.0.0.1:5557", pub_addr = "tcp://127.0.0.1:5558", clear_screen = False, n_workers = 1, thread_safe = True)
    server.register_function(add)
    server.register_function(subtract)
    server.register_class(ExampleClass)  

    server.serve()



if __name__ == "__main__":

    test_server()

