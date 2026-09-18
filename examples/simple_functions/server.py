from srpc import SRPCServer


def add(a, b):
    return a + b


class Calculator:
    def multiply(self, a, b):
        return a * b


if __name__ == "__main__":
    server = SRPCServer("calculator", clear_screen=False, n_workers=2)
    server.register_function(add)
    server.register_class(Calculator)
    server.serve()
