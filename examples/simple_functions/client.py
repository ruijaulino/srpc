import srpc

if __name__ == "__main__":
    client = srpc.SRPCClient(host = "localhost", port = 5557)    
    print(client.call("add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
    print(client.call("subtract", [10, 43]))  # Output: {'result': 6}
    print(client.call("multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
    print(client.call("ExampleClass.multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
