import zmq
import time

C_CLIENT = b"C_CLIENT"



def encode(msg):
    '''
    msg: str or list of strings
    '''

    enc_msg = "\x01".encode()
    if isinstance(msg, str):
        enc_msg = msg.encode()
    elif isinstance(msg, list):
        enc_msg = []
        for e in msg: 
            if isinstance(e, str):
                e = e.encode()
            else:
                assert isinstance(e, bytes), "could not encode message"
            enc_msg.append(e)
    else:
        # not string, not list
        assert isinstance(msg, bytes), "could not encode message"
        enc_msg = msg    
    return enc_msg   


def dump(msg):
    if isinstance(msg, str):
        return 

def main():
    """create and start new broker"""

    addr = "tcp://127.0.0.1:5555"

    ctx = zmq.Context()

    client = ctx.socket(zmq.REQ)
    client.linger = 0
    
    # client.setsockopt_string(zmq.IDENTITY, "random_client")

    client.connect(addr)
    # poller.register(client, zmq.POLLIN)
    # poller.register(client, zmq.POLLOUT)

    service = "echo"

    request = "ola"

    if not isinstance(request, list):
        request = [request]
    request = [C_CLIENT, service] + request

    request = encode(request)
    
    client.send_multipart(request)

    # wait for reply
    rep = None
    if (client.poll(2000) & zmq.POLLIN) != 0:
        rep = client.recv_multipart()

    print('response: ', rep)

    # poller.unregister(client)
    client.close()

    ctx.destroy()

if __name__ == '__main__':
    main()
