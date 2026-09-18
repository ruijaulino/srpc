from srpc import SRPCClient


if __name__ == "__main__":
    with SRPCClient(timeo=2) as client:
        print(client.invoke("calculator", "add", args=[1, 2], raise_errors=True))
        print(client.invoke("calculator", "Calculator.multiply", args=[3, 4]))
