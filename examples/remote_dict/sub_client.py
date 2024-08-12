import srpc
import time
import datetime as dt
import numpy as np

if __name__ == '__main__':   
    client = srpc.StoreClient("localhost", 4005, sub_port = 4006)    
    client.subscribe(topic = 'topic_pub')
    while True:
        topic, msg = client.listen()
        if topic is not None:
            print(dt.datetime.now(), topic, msg)
    client.close()
   