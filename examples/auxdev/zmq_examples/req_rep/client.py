import zmq


#  Prepare our context and sockets
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5560")

#  Do 10 requests, waiting each time for a response
for request in range(1, 11):
    socket.send_multipart([b'',b"Hello"])
    message = socket.recv_multipart()
    print(f"Received reply {request} [{message}]")