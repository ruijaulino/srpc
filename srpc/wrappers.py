import time
import json
import zmq
import signal
import threading
import sys
import os
from multiprocessing import Process# , Queue
import queue
from queue import Queue

def clear_screen():
    # For Windows
    if os.name == 'nt':
        os.system('cls')
    # For macOS and Linux
    else:
        os.system('clear')


class QueueWrapper(object):
    def __init__(self, max_size=2048):
        self.q = Queue(max_size)
        self.active = threading.Event()
        self.active.set()


    def close(self):
        pass
        # self.active.clear()
        # self.q.close() 
        # self.q.join_thread()

    def get(self, timeout=0):
        out = None
        # if self.active.isSet():
        try:
            out = self.q.get(True, timeout)
        except queue.Empty:
            pass
        return out

    def get_all(self, timeout=0):
        out = []
        while True:
            tmp = self.get(timeout=timeout)
            if tmp is not None:
                out.append(tmp)
            else:
                break
        if len(out) == 0:
            out = None
        else:
            return out

    def put(self, obj, timeout=0):
        status = 1
        # if self.active.isSet():
            # with self.lock:
        try:
            self.q.put(obj, True, timeout)
            status = 0
        except queue.Full:
            pass
        return status



class SRPCTopic:
    
    def __init__(self, *args):
        self.sep = '.'
        self.parts = []
        for e in [str(a) for a in args]: self.parts += [self._fix_str(b) for b in e.split(self.sep)]
        self.topic = '.'.join(self.parts)

    def _fix_str(self, s:str):
        s = s.lower() # in lower case
        s = s.replace(' ','_') # replace spaces otherwise it will mess the topics
        return s
    
    def __str__(self):
        return self.topic

class SocketPub:
    def __init__(self, host:str, port:int):

        self.host = host
        self.port = port
        # build address
        self.addr = f"tcp://{host}:{port}"    
        # replace localhost
        self.addr = self.addr.replace('localhost','127.0.0.1')
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(self.addr)
        self.connected = True

    def publish(self, topic:SRPCTopic, value:any):
        if isinstance(topic, str):
            topic = SRPCTopic(topic)
        if self.connected:
            try:
                msg = f"{topic.topic} {value}"
                self.socket.send_string(msg)
            except:
                pass
                
    def close(self):
        if self.connected:
            self.socket.close()
            self.context.term()
            self.connected = False


class SocketSub:
    def __init__(self, host:str, port:int, recvtimeo:int = 100, last_msg_only:bool = True):
        
        self.host = host
        self.port = port
        # build address
        self.addr = f"tcp://{host}:{port}"    
        # replace localhost
        self.addr = self.addr.replace('localhost','127.0.0.1')
        self.recvtimeo = recvtimeo
        self.conflate = 1 if last_msg_only else 0
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.CONFLATE, self.conflate) 
        self.socket.connect(self.addr)
        self.topics = [] #
        if self.recvtimeo is not None:
            self.socket.setsockopt(zmq.RCVTIMEO, self.recvtimeo)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.connected = True

    def _is_subscribed(self, topic:SRPCTopic):
        for t in self.topics: 
            if t.topic == topic.topic:
                return True
        return False

    def _delete_topic(self, topic:SRPCTopic):
        idx = None
        for i in range(len(self.topics)):
            if self.topics[i].topic == topic.topic:
                idx = i
                break
        if idx is not None:
            del self.topics[idx]

    def unsubscribe(self, topic:SRPCTopic):
        if isinstance(topic, str):
            topic = SRPCTopic(topic)        
        if self.connected and self._is_subscribed(topic):
            self.socket.setsockopt_string(zmq.UNSUBSCRIBE, topic.topic)
            self._delete_topic(topic = topic)    

    def subscribe(self, topic:SRPCTopic, unsubscribe = True):
        if isinstance(topic, str):
            topic = SRPCTopic(topic)
        if self.connected and not self._is_subscribed(topic):
            if unsubscribe:
                self.unsubscribe(topic = topic)
            self.topics.append(topic)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic.topic)
    
    def recv(self):
        if len(self.topics)==0: return None,None
        try:     
            msg = self.socket.recv_string()
            topic, msg = msg.split(' ', 1)
            return SRPCTopic(topic), msg
        except zmq.Again:
            return None, None
    
    def close(self):
        self.connected = False
        self.socket.close()
        self.context.term()

class SocketReqRep:
    """
    Wrapper over ZMQ REP/REQ Socket.
    """
    
    def __init__(self, host:str, port:int, zmq_type:str, bind:bool, recvtimeo:int = 1000, sndtimeo:int = 100, reconnect:int = 60*60):
        """
        Initialize the ZMQRSocket.
        """
        self.host = host
        self.port = port
        # build address
        self.addr = f"tcp://{host}:{port}"    
        # replace localhost
        self.addr = self.addr.replace('localhost','127.0.0.1')
        assert zmq_type in ['REP','REQ'], f"Unknown zmq_type {zmq_type}. Use REP or REP"
        self.zmq_type = zmq.REP if zmq_type == 'REP' else zmq.REQ
        self.recvtimeo = recvtimeo
        self.sndtimeo = sndtimeo
        self.reconnect = reconnect
        self.connected = False
        self.bind = bind
        self.connect()

    def connect(self) -> None:
        """
        Connect to the ZMQ socket.
        """
        self.last_connect = time.time()
        if self.connected:
            self.close()
        self.context = zmq.Context()
        self.socket = self.context.socket(self.zmq_type)
        if self.bind:
            self.socket.bind(self.addr)        
        else:
            self.socket.connect(self.addr)  
        if self.recvtimeo is not None:
            self.socket.setsockopt(zmq.RCVTIMEO, self.recvtimeo)
        if self.sndtimeo is not None:
            self.socket.setsockopt(zmq.SNDTIMEO, self.sndtimeo)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.connected = True

    def close(self) -> None:
        """
        Close the ZMQ socket and terminate the context.
        """
        self.socket.close()
        self.context.term()
        self.connected = False

    def recv(self) -> any:
        """
        Receive a message from the ZMQ socket.
        """                         
        if self.zmq_type == zmq.REP and time.time() - self.last_connect > self.reconnect:
            self.connect()   
        msg = None        
        try:     
            msg = self.socket.recv_string()
        except zmq.Again:
            if self.zmq_type == zmq.REQ:
                self.close()
                self.connect()
        return msg

    def send(self, msg:str) -> int:
        """
        Send a message through the ZMQ socket.

        :param msg: The message to send.
        :param obj: Whether to treat the message as an object.
        :return: Status code, 1 for success, 0 for failure.
        """
        if self.zmq_type == zmq.REQ and time.time() - self.last_connect > self.reconnect:
            self.connect()        
        status = 0
        try:
            self.socket.send_string(msg)
            status = 1
        except zmq.Again:
            if self.zmq_type == zmq.REP:
                self.close()
                self.connect()        
        return status


if __name__ == '__main__':
    topic = SRPCTopic('ib','ESU4 Index')
    print(topic)
    print(topic.parts)

