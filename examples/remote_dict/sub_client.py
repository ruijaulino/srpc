import srpc
import time
import datetime as dt
import numpy as np

if __name__ == '__main__':   
    client = srpc.StoreClient(req_addr = "tcp://127.0.0.1:5551", sub_addr = "tcp://127.0.0.1:5552")
    client.subscribe(topic = "topic_pub")
    while True:
        topic, msg = client.listen()
        if topic is not None:
            print(dt.datetime.now(), topic, msg)
    client.close()
   