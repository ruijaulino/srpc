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

# class to build topic string
class SRPCTopic:
    
    def __init__(self, *args):
        self.sep = '.'
        self.parts = []
        for e in [str(a) for a in args]: self.parts += [self._fix_str(str(b)) for b in e.split(self.sep)]
        self.topic = '.'.join(self.parts)

    def _fix_str(self, s:str):
        s = s.lower() # in lower case
        s = s.replace(' ','_') # replace spaces otherwise it will mess the topics
        s = s.replace('/','_')
        return s
    
    def __str__(self):
        return self.topic

def clear_screen():
    # For Windows
    if os.name == 'nt':
        os.system('cls')
    # For macOS and Linux
    else:
        os.system('clear')
