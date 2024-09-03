import itertools
import sys
import zmq
import custom_zmq
import json

REQUEST_TIMEOUT = 10
REQUEST_RETRIES = 3

addr = "tcp://127.0.0.1:5555"
ctx = zmq.Context()
s = custom_zmq.ZMQR(ctx, zmq.REQ, timeo = REQUEST_TIMEOUT)
s.connect(addr)

abort = False

for sequence in range(1):
    if abort: break
    request = 'request ' + str(sequence)
    request = {'request':sequence, 'hey':"ola"}
    request = json.dumps(request)
    print("Sending (%s)", request)
    s.send(request)

    retries_left = REQUEST_RETRIES
    while True:
        reply = s.recv()
        if reply is not None:
            print("Server replied: %s"% reply)
            retries_left = REQUEST_RETRIES
            break
            
        else:
            print("No response from server")
            retries_left -= 1
            if retries_left == 0:
                print("Server seems to be offline, abandoning")
                abort = True
                break

s.close()
ctx.term()