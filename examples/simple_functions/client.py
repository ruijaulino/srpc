import srpc

client = srpc.SRPCClient(req_addr = "tcp://127.0.0.1:5557", sub_addr = "tcp://127.0.0.1:5558")

print(client.call("add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
print(client.call("subtract", [10, 43]))  # Output: {'result': -33}
print(client.call("multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
print(client.call("ExampleClass.multiply", [3, 4]))  # Output: {'result': 12}
print(client.call("Store.set", ["ola", 1]))  # Output: {'result': 12}
print(client.call("Store.get", ["ola"]))  # Output: {'result': 12}


