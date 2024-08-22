import srpc

if __name__ == '__main__':
    server = srpc.Store(rep_addr = "tcp://127.0.0.1:5551", pub_addr = "tcp://127.0.0.1:5552")  
    server.serve()
