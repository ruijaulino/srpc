import srpc

if __name__ == '__main__':
    server = srpc.Store(service_name = 'store', host = 'localhost', port = 4005, pub_port = 4006)  
    server.srpc_serve()
