import srpc

if __name__ == '__main__':   

    client = srpc.StoreClient("localhost", 4005)
    print(client.set('ola',1))
    print(client.get('ola'))
    print(client.get('ole'))
    print(client.keys())
    print(client.delete('ola'))
    print(client.keys())
    client.close()
   