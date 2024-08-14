import zmq

def main():
    context = zmq.Context()

    # Create a REQ (request) socket
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")  # Connect to the server

    # Send a request and wait for a response
    for request in range(1, 6):
        message = f"Hello {request}"
        print(f"Sending request {request}: {message}")
        socket.send_string(message)
        
        # Wait for the reply from the server
        reply = socket.recv_string()
        print(f"Received reply {request}: {reply}")

if __name__ == "__main__":
    main()
