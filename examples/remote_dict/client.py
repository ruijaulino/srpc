import srpc

if __name__ == '__main__':   

    client = srpc.StoreClient(req_addr = "tcp://127.0.0.1:5551", sub_addr = "tcp://127.0.0.1:5552")
    print(client.set('ola',1))
    print(client.get('ola'))
    print(client.get('ole'))
    print(client.keys())
    print(client.delete('ola'))
    print(client.keys())
    print()
    client.close()
   