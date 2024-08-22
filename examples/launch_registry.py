import srpc

if __name__ == '__main__':
	registry = srpc.SRPCRegistry()
	registry.serve()