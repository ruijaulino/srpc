from random import randint
import time
import zmq
import custom_zmq
from custom_zmq import ZMQR

WORKER_READY = "\x01"

# Reliable worker pattern
class Worker():
	def __init__(self, ctx:zmq.Context, timeo:int = 1):
		self.socket = custom_zmq.ZMQR(ctx, zmq.REQ, timeo = time, identity = identity, reconnect = False)

	def connect(self, addr):
		self.socket.connect(addr)
		self.socket.send(WORKER_READY)

	def start():
		pass

	def stop(self):
		pass

LRU_READY = "\x01"

addr = "tcp://127.0.0.1:5556"
ctx = zmq.Context()

identity = "%04X-%04X" % (randint(0, 0x10000), randint(0,0x10000))
s = custom_zmq.ZMQR(ctx, zmq.REQ, identity = identity, reconnect = False)
s.connect(addr)

print("I: (%s) worker ready" % identity)
s.send(LRU_READY)

cycles = 0
while True:
    try:
        msg = s.recv_multipart()
        if not msg:
            s.send(LRU_READY)
            pass
        else:
            print('MSG: ', msg)
            cycles += 1
            if cycles>0 and randint(0, 5) == 0:
                print("I: (%s) simulating a crash" % identity)
                break
            elif cycles>3 and randint(0, 5) == 0:
                print("I: (%s) simulating CPU overload" % identity)
                time.sleep(3)
            print("I: (%s) normal reply" % identity)
            time.sleep(1) # Do some heavy work
            #print('ola')
            msg[-1] = "reply to request ".encode()
            #print('ole')
            s.send_multipart(msg)
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        break            
s.close()
ctx.term()



