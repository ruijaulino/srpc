import srpc
import time
import datetime as dt
import numpy as np

if __name__ == '__main__':   
    client = srpc.StoreClient("localhost", 4005)    
    c = 0
    while True:

        print('[%s] Publish'%dt.datetime.now(), c)
        client.publish('topic_pub', c)
        c += 1
        time.sleep(1)
    client.close()
   