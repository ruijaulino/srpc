import zmq
import json
import time
import datetime as dt
from typing import Deque, Dict, List, Optional, Tuple, Union
try:
    from .custom_zmq import ZMQSub, ZMQServiceBrokerClient
    from .utils import build_server_response, OK_STATUS, ERROR_STATUS
    from .defaults import NO_REP_MSG, NO_REQ_MSG
    from .defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR
except ImportError:
    from custom_zmq import ZMQSub, ZMQServiceBrokerClient
    from utils import build_server_response, OK_STATUS, ERROR_STATUS
    from defaults import NO_REP_MSG, NO_REQ_MSG
    from defaults import BROKER_ADDR, PROXY_PUB_ADDR, PROXY_SUB_ADDR


def ts():
    return dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')




class SRPCClient:
    def __init__(
        self,
        broker_addr: Optional[str] = None,
        proxy_pub_addr: Optional[str] = None,
        timeo: int = 1,
        last_msg_only: bool = True,
        no_rep_msg: Optional[str] = None,
        no_req_msg: Optional[str] = None,
        info: bool = True,
    ):
        self._broker_addr = broker_addr or BROKER_ADDR
        self._proxy_pub_addr = proxy_pub_addr or PROXY_PUB_ADDR
        self._timeo = timeo
        self._last_msg_only = last_msg_only

        self.no_rep_msg = no_rep_msg or NO_REP_MSG
        self.no_req_msg = no_req_msg or NO_REQ_MSG
        self.info = info

        self._closed = True

        self.reconnect()

        # Client owns its context.
        #self.ctx = zmq.Context()

        #self._broker_client = ZMQServiceBrokerClient(self.ctx, info=info)
        #self._broker_client.connect(self._broker_addr)

        #self._proxy_sub = ZMQSub(
        #    ctx=self.ctx,
        #    last_msg_only=self._last_msg_only,
        #    timeo=self._timeo,
        #)
        #self._proxy_sub.connect(self._proxy_pub_addr)

    def reconnect(self):
        
        self.close()

        self.ctx = zmq.Context()

        self._broker_client = ZMQServiceBrokerClient(self.ctx, info=self.info)
        self._broker_client.connect(self._broker_addr)

        self._proxy_sub = ZMQSub(
            ctx=self.ctx,
            last_msg_only=self._last_msg_only,
            timeo=self._timeo,
        )
        self._proxy_sub.connect(self._proxy_pub_addr)
        self._closed = False

    def set_timeo(self, timeo: int) -> None:
        self._timeo = timeo
        self._proxy_sub.timeo = timeo

    def close(self) -> None:
        if not self._closed:
            self._broker_client.close()
            self._proxy_sub.close()
            self.ctx.term()
            self._closed = True

    def subscribe(self, topic:str):
        self._proxy_sub.subscribe(topic)

    def listen(self):
        try:
            topic, msg = self._proxy_sub.recv()        
        except:
            topic, msg = -1, None
        return topic, msg

    # must be subscribed before
    # waits for a topic, this emulates a large timeout on the sub socket but its cleaner to handle
    def wait(self): 
        # if there is a KeyboardInterrupt (or other error) we return a topic = -1
        try:
            topic, msg = None, None
            while True:
                topic, msg = self.listen()            
                if topic is not None:
                    return topic, msg
        except:
            topic, msg = -1, None
        return topic, msg


    def parse(self, rep: Optional[dict]):
        if not isinstance(rep, dict):
            return self.no_rep_msg

        if rep.get("status") == OK_STATUS:
            return rep.get("output")

        return rep.get("error_msg", self.no_rep_msg)

    # -------------------
    def _build_request(self, method: str, args=None, kwargs=None) -> str:
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs

        return json.dumps({
            "method": method,
            "args": args,
            "kwargs": kwargs,
        })

    def send(
        self,
        service: str,
        method: str,
        args=None,
        kwargs=None,
    ) -> Optional[str]:
        """
        Send a request without waiting for the response.

        Returns:
            request id, or None if sending failed.
        """
        req = self._build_request(
            method=method,
            args=args,
            kwargs=kwargs,
        )

        return self._broker_client.req(
            service=service,
            msg=req,
            timeo=self._timeo,
        )
    # -------------------

    # -------------------
    def _error_response(self, message: str) -> dict:
        return build_server_response(
            status=ERROR_STATUS,
            output=None,
            error_msg=message,
        )

    def _decode_reply(
        self,
        reply: Optional[dict],
        expected_req_id: Optional[str] = None,
    ) -> dict:
        if reply is None:
            return self._error_response(self.no_rep_msg)

        req_id = reply.get("req_id")
        raw = reply.get("rep")

        if expected_req_id is not None and req_id != expected_req_id:
            if self.info:
                print(
                    "WARNING: received response for a different request id. "
                    "There may be stale messages in the socket - reconnect..."
                )
            # reconnect the socket here
            # this never creates an issue...
            self.reconnect()
            return self._error_response(self.no_rep_msg)

        if raw is None or raw == "ERROR":
            return self._error_response(self.no_rep_msg)

        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            return self._error_response(f"Invalid JSON response: {exc}")

    def receive(
        self,
        req_id: str,
        close: bool = False,
    ) -> dict:
        """
        Receive a response for a specific request id.

        Returns a normalized server response dict.
        """
        try:
            reply = self._broker_client.rep(timeo=self._timeo)
            return self._decode_reply(reply, expected_req_id=req_id)
        finally:
            if close:
                self.close()
    # -------------------

    def receive_any(
        self,
        timeo = None,
        close: bool = False,
    ) -> dict:
        """
        Receive any available response.

        Returns:
            {
                "req_id": req_id,
                "output": parsed_output_or_error
            }
        """
        try:
            reply = self._broker_client.rep(timeo=timeo or self._timeo)

            if reply is None:
                return {
                    "req_id": None,
                    "output": self.no_rep_msg,
                }

            req_id = reply.get("req_id")
            response = self._decode_reply(reply)

            return {
                "req_id": req_id,
                "output": self.parse(response),
            }

        finally:
            if close:
                self.close()

    def invoke(
        self,
        service: str,
        method: str,
        args=None,
        kwargs=None,
        close: bool = False,
    ):
        """
        Send request, wait for matching response, and return parsed output.
        """
        start = time.time()

        req_id = self.send(
            service=service,
            method=method,
            args=args,
            kwargs=kwargs,
        )

        if req_id is None:
            out = self.no_req_msg
        else:
            response = self.receive(req_id=req_id, close=close)
            out = self.parse(response)

        if self.info:
            print(
                f"[{ts()}] Time to invoke method {method} "
                f"on service {service}: {time.time() - start} [secs]"
            )

        return out

    def invoque(self, *args, **kwargs):
        """
        Backwards-compatible alias.
        Prefer invoke().
        """
        return self.invoke(*args, **kwargs)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()















#############################################
#############################################
#############################################


# class SRPCClient:
#     def __init__(self, broker_addr:str = None, proxy_pub_addr:str = None, timeo:int = 1, last_msg_only:bool = True, no_rep_msg:str = None, no_req_msg:str = None, info:bool = True):
        
#         self._broker_addr = broker_addr if broker_addr else BROKER_ADDR
#         self._proxy_pub_addr = proxy_pub_addr if proxy_pub_addr else PROXY_PUB_ADDR
#         self._timeo = timeo
#         self._last_msg_only = last_msg_only
#         self.no_rep_msg = no_rep_msg if no_rep_msg else NO_REP_MSG
#         self.no_req_msg = no_req_msg if no_req_msg else NO_REQ_MSG
        
#         self.info = info
#         # it is better that the clients create their own, non shared context, in order to be able to use
#         # clients inside other SRPCServer's
#         self.ctx = zmq.Context()
        
#         self._broker_client = ZMQServiceBrokerClient(self.ctx, info = info)
#         self._broker_client.connect(self._broker_addr)
        
#         self._proxy_sub = ZMQSub(ctx = self.ctx, last_msg_only = self._last_msg_only, timeo = self._timeo)
#         self._proxy_sub.connect(self._proxy_pub_addr)


#     def set_timeo(self, timeo:int):
#         self._timeo = timeo

#     def close(self):
#         # if the client (or some class that inherits from SRPCClient) 
#         # is being used where another shared context exits, then we must take
#         # case not to terminate the context here as it may break the code 
#         # when we try to close it!
#         self._broker_client.close()
#         self._proxy_sub.close()
#         self.ctx.term()

#     def subscribe(self, topic:str):
#         self._proxy_sub.subscribe(topic)

#     def listen(self):
#         try:
#             topic, msg = self._proxy_sub.recv()        
#         except:
#             topic, msg = -1, None
#         return topic, msg

#     # must be subscribed before
#     # waits for a topic (could just increase the timeout)
#     def wait(self, timeo:int = 3000000): # the timeout is very large. in any practical setting this is not achieved
#         # if there is a KeyboardInterrupt (or other error) we return a topic = -1
#         try:
#             topic, msg = None, None
#             s = time.time()
#             while time.time() < s + timeo:
#                 topic, msg = self.listen()            
#                 if topic is not None:
#                     return topic, msg
#         except:
#             topic, msg = -1, None
#         return topic, msg

#     # def call_working(self, service, method, args = [], kwargs = {}, close:bool = False):
#     #     '''
#     #     call a method on a service and wait for response
#     #     '''
#     #     req = {
#     #             "method": method,
#     #             "args": args,
#     #             "kwargs": kwargs
#     #         }
#     #     req = json.dumps(req)
#     #     # Send the request
#     #     req_id = self._broker_client.req(service = service, msg = req, timeo = self._timeo)
#     #     # reply with the request id associated!
#     #     if req_id:
#     #         rep = self._broker_client.rep(timeo = self._timeo)                        
#     #         if rep is not None:                
#     #             if rep != "ERROR":
#     #                 rep = json.loads(rep)
#     #             else:
#     #                 rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)    
#     #         else:
#     #             # this should emulate the response from the server
#     #             rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
#     #     else:
#     #         # this should emulate the response from the server
#     #         rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_req_msg)
#     #     if close: self.close()
#     #     return rep

#     # implement a method to parse the responses from the server
#     # this seems to be practical
#     def parse(self, rep):
#         if rep.get('status') == OK_STATUS:
#             return rep.get('output')
#         else:
#             return rep.get('error_msg')

#     def call(self, service, method, args = [], kwargs = {}):
#         '''
#         call a method on a service
#         '''
#         req = {
#                 "method": method,
#                 "args": args,
#                 "kwargs": kwargs
#             }
#         req = json.dumps(req)
#         # Send the request
#         req_id = self._broker_client.req(service = service, msg = req, timeo = self._timeo)
#         return req_id

#     def collect_(self, req_id = None, close = False):
#         # if req_id is not None then we need the output to be equal. do not use this when requesting asynch!
#         # this is just being used to check if the requests made with collect are correct!

#         # get request
#         rep = self._broker_client.rep(timeo = self._timeo)                                
#         # output should be a dict
#         if isinstance(rep, dict):
#             if req_id is not None:
#                 if rep.get('req_id') == req_id:
#                     rep = rep.get('rep')
#                     if rep != "ERROR":
#                         rep = json.loads(rep)
#                     else:
#                         rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)                    
#                 else:                    
#                     print('WARNING: RECEIVING RESPONSES TO A REQUEST THAT WAS NOT MADE. PROBABLY THERE ARE MESSAGES STUCK IN THE SOCKET. RESET')
#                     rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
#             else:
#                 # this is to collect message, output here is different
#                 req_id = rep.get('req_id')                
#                 rep = rep.get('rep')
#                 if rep != "ERROR":
#                     rep = json.loads(rep)
#                 else:
#                     rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)                    
#                 rep = {'req_id':req_id, 'output':self.parse(rep)}
#         else:
#             if req_id is not None:
#                 # this should emulate the response from the server
#                 rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
#             # when collecting...
#             else:
#                 rep = {'req_id':req_id, 'output':self.no_rep_msg}
#         if close: self.close()
#         return rep

#     def collect(self, close = False):
#         return self.collect_(None, close)

#     def invoque(self, service, method, collect = True, args = [], kwargs = {}, close = False):
#         s = time.time()
#         if close: collect = True
#         req_id = self.call(service, method, args, kwargs)
        
#         if collect:
            
#             out = self.parse(self.collect_(req_id, close))
#         else:
#             out = self.no_req_msg
#             if req_id is not None:
#                 out = req_id                 
#         if self.info:
#             print(f'[{ts()}] Time to invoque method {method} on service {service}: {time.time()-s} [secs]')
        
#         return out


#     # def call(self, service, method, args = [], kwargs = {}, close:bool = False):
#     #     '''
#     #     call a method on a service
#     #     '''
#     #     req = {
#     #             "method": method,
#     #             "args": args,
#     #             "kwargs": kwargs
#     #         }
#     #     req = json.dumps(req)
#     #     # Send the request
#     #     req_id = self._broker_client.req(service = service, msg = req, timeo = self._timeo)
#     #     # reply with the request id associated!
#     #     if req_id:
#     #         rep = self._broker_client.rep(timeo = self._timeo)                        
#     #         # output should be a dict
#     #         if isinstance(rep, dict):
#     #             # check if we are getting the response to the same request that was made
#     #             if rep.get('req_id') == req_id:
#     #                 rep = rep.get('rep')
#     #                 if rep != "ERROR":
#     #                     rep = json.loads(rep)
#     #                 else:
#     #                     rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)    
#     #             else:
#     #                 print('WARNING: RECEIVING RESPONSES TO A REQUEST THAT WAS NOT MADE. PROBABLY THERE ARE MESSAGES STUCK IN THE SOCKET. RESET')
#     #                 rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
#     #         else:
#     #             # this should emulate the response from the server
#     #             rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_rep_msg)
#     #     else:
#     #         # this should emulate the response from the server
#     #         rep = build_server_response(status = ERROR_STATUS, output = None, error_msg = self.no_req_msg)
#     #     if close: self.close()
#     #     return rep


#     # # implement a method to parse the responses from the server
#     # # this seems to be practical
#     # def parse(self, rep):
#     #     if rep.get('status') == OK_STATUS:
#     #         return rep.get('output')
#     #     else:
#     #         return rep.get('error_msg')

#     # def collect(self, ):
#     #     pass

#     # def invoque(self, service, method, args = [], kwargs = {}, close = False):
#     #     s = time.time()
#     #     out = self.parse(self.call(service, method, args, kwargs, close))
#     #     if self.info:
#     #         print(f'[{ts()}] Time to invoque method {method} on service {service}: {time.time()-s} [secs]')
#     #     return out




def test_client():
    client = SRPCClient()
    print(client.invoque("test_server","add", kwargs = {"a":1,"b":2}))  # Output: {'result': 3}
    print(client.invoque("test_server","subtract", [10, 43]))  # Output: {'result': -33}
    print(client.invoque("test_server","multiply", [2, 3]))  # Output: {'error': 'Unknown method: multiply'}
    print(client.invoque("test_server","ExampleClass.multiply", [3, 4]))  # Output: {'result': 12}
    print(client.invoque("test_server","Store.set", ["ola", 1]))  # Output: {'result': 12}
    print(client.invoque("test_server","Store.get", ["ola"]))  # Output: {'result': 12}

if __name__ == "__main__":

    test_client()

