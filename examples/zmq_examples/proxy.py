
from custom_zmq import proxy
import zmq




pub_addr = "tcp://127.0.0.1:5556"
sub_addr = "tcp://127.0.0.1:5557"
proxy(pub_addr, sub_addr)

