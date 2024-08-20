import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://localhost:5560")


socket.setsockopt(zmq.RCVTIMEO, 20000)
socket.setsockopt(zmq.SNDTIMEO, 20000)
socket.setsockopt(zmq.LINGER, 0)

while True:
    message = socket.recv()
    print(f"Received request: {message}")
    time.sleep(5)
    print("Send reply")    
    socket.send(b"World")
    print()