import custom_zmq
import zmq

addr = "tcp://127.0.0.1:5555"
ctx = zmq.Context()
s = custom_zmq.ZMQR(ctx, zmq.REP)
s.bind(addr)

while True:
	try:
		req = s.recv(1)
		if req is not None:
			print('recv: ', req)
			rep = "im alive"
			s.send(rep)
	except KeyboardInterrupt:
		print('detected KeyboardInterrupt')
		break
s.close()
ctx.term()
