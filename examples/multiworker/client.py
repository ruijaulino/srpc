from srpc import SRPCClient
import time
import threading


def make_req():
    # services broker
    SERVICES_BROKER_ADDR = f"tcp://192.168.2.152:5000"
    SERVICES_PROXY_PUB_ADDR = f"tcp://192.168.2.152:5001"
    SERVICES_PROXY_SUB_ADDR = f"tcp://192.168.2.152:5002"

    client = SRPCClient(broker_addr = SERVICES_BROKER_ADDR, proxy_pub_addr = SERVICES_PROXY_PUB_ADDR, timeo = 2)

    out = client.invoque("ExampleMultiworker","somemethod", kwargs = {"param":0})
    print(out)
    client.close()

if __name__ == '__main__':

    s = time.time()

    n = 10
    ths = []
    for i in range(n):
        th = threading.Thread(target = make_req)
        th.start()
        ths.append(th)
    for th in ths:
        th.join()


    print('Elapsed time: ', time.time()-s)

