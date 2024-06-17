import time
import json
import zmq
import signal
import threading
import sys
import os

def clear_screen():
    # For Windows
    if os.name == 'nt':
        os.system('cls')
    # For macOS and Linux
    else:
        os.system('clear')


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

    def publish(self, key:str, value:any):
        if self.connected:
            msg = f"{key} {value}"
            self.socket.send_string(msg)
    
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
        self.key = None
        if self.recvtimeo is not None:
            self.socket.setsockopt(zmq.RCVTIMEO, self.recvtimeo)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.connected = True

    def subscribe(self, key):
        if self.connected:
            if self.key is not None:
                self.socket.setsockopt_string(zmq.UNSUBSCRIBE, self.key)
                # print(f"Unsubscribed from key: {self.key}")
            self.key = key
            self.socket.setsockopt_string(zmq.SUBSCRIBE, self.key)
            # print(f"Subscribed to key: {self.key}")
    
    def recv(self):
        if self.key is None: return None,None
        try:     
            msg = self.socket.recv_string()
            key, msg = msg.split(' ', 1)
            return key, msg
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
