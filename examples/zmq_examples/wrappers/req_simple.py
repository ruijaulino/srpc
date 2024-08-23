import custom_zmq
import zmq
import json

addr = "tcp://127.0.0.1:5555"
ctx = zmq.Context()
s = custom_zmq.ZMQR(ctx, zmq.REQ)
s.connect(addr)

req = "hi"
req = {'ola':1, 'ole':2.5}
req = json.dumps(req)
s.send(req)
rep = s.recv()
print('recv: ', rep)


s.close()
ctx.term()
