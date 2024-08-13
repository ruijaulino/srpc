import srpc

def add(a, b):
    return a + b

def subtract(a, b):
    return a - b

class ExampleClass:
    def multiply(self, a, b):
        return a * b

    def divide(self, a, b):
        if b == 0:
            return "Cannot divide by zero"
        return a / b

if __name__ == "__main__":
    server = srpc.SRPCServer(name = 'test_server', host = "localhost", port = 5557)
    server.register_function(add)
    server.register_function(subtract)
    server.register_class(ExampleClass)  
    server.serve()