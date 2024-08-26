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

server = srpc.SRPCServer(name = 'test_server', rep_addr = "tcp://127.0.0.1:5557", pub_addr = "tcp://127.0.0.1:5558", clear_screen = False, n_workers = 5, thread_safe = True)

server.register_function(add)
server.register_function(subtract)
server.register_class(ExampleClass)  

server.serve()

