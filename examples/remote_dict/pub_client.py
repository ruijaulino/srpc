import srpc
import time
import datetime as dt
import numpy as np

if __name__ == '__main__':   
    client = srpc.StoreClient(req_addr = "tcp://127.0.0.1:5551", sub_addr = "tcp://127.0.0.1:5552")
    c = 0
    while True:
        print('[%s] Publish'%dt.datetime.now(), c)
        client.publish('topic_pub', c)
        c += 1
        time.sleep(5)
    client.close()
   