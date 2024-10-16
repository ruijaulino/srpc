import srpc

if __name__ == '__main__':
    server = srpc.Store(service_name = 'TestStore')  
    server.serve()
