import zmq
from srpc import SocketReqRep

def main():
    # context = zmq.Context()

    # Create a REQ (request) socket
    # socket = context.socket(zmq.REQ)
    # socket.connect("tcp://127.0.0.1:5555")  # Connect to the server

    socket = SocketReqRep(zmq_type = 'REQ', bind = False, addr = "tcp://127.0.0.1:5555", recvtimeo = 10000)


    # Send a request and wait for a response
    for request in range(1, 6):
        message = f"Hello {request}"
        # print(f"Sending request {request}: {message}")
        status = socket.send(message)
        print('send status: ', status)
        # socket.send_string(message)
        if status == 1:
            # Wait for the reply from the server
            reply = socket.recv()
            if reply is not None:
                print(f"Received reply {request}: {reply}")

if __name__ == "__main__":
    main()
