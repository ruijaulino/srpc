import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://127.0.0.1:5560")

while True:
    message = socket.recv_multipart()
    print(f"Received request: {message}")
    socket.send_multipart([b'',b"World"])
