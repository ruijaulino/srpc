import zmq
import json
import time
import datetime as dt
from typing import Optional
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




class RPCError(RuntimeError):
    """A remote call failed; the structured envelope is available as response."""

    def __init__(self, response):
        self.response = response
        super().__init__(response.get("error_msg", "RPC failed"))


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
        if timeo < 0:
            raise ValueError("timeo must be nonnegative seconds")
        self._broker_addr = broker_addr or BROKER_ADDR
        self._proxy_pub_addr = proxy_pub_addr or PROXY_PUB_ADDR
        self._timeo = timeo
        self._last_msg_only = last_msg_only

        self.no_rep_msg = no_rep_msg or NO_REP_MSG
        self.no_req_msg = no_req_msg or NO_REQ_MSG
        self.info = info

        self._closed = True
        self._subscriptions = set()
        self._replies = {}

        self.reconnect()

    def reconnect(self):
        
        self.close()
        self._replies.clear()

        self.ctx = zmq.Context()

        self._broker_client = ZMQServiceBrokerClient(self.ctx, info=self.info)
        self._broker_client.connect(self._broker_addr)

        self._proxy_sub = ZMQSub(
            ctx=self.ctx,
            last_msg_only=self._last_msg_only,
            timeo=self._timeo,
        )
        self._proxy_sub.connect(self._proxy_pub_addr)
        for topic in self._subscriptions:
            self._proxy_sub.subscribe(topic)
        self._closed = False

    def set_timeo(self, timeo: float) -> None:
        if timeo < 0:
            raise ValueError("timeo must be nonnegative seconds")
        self._timeo = timeo
        self._proxy_sub.timeo = int(1000 * timeo)

    def close(self) -> None:
        if not self._closed:
            self._broker_client.close()
            self._proxy_sub.close()
            self.ctx.term()
            self._closed = True

    def subscribe(self, topic:str):
        self._proxy_sub.subscribe(topic)
        self._subscriptions.add(topic)

    def unsubscribe(self, topic):
        self._proxy_sub.unsubscribe(topic)
        self._subscriptions.discard(topic)

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
            return self._error_response("Response request ID does not match")
        if isinstance(raw, str) and raw.startswith("ERR:"):
            return self._error_response(raw[4:])

        if raw is None or raw == "ERROR":
            return self._error_response(self.no_rep_msg)

        try:
            response = json.loads(raw)
            if not isinstance(response, dict) or response.get("status") not in (OK_STATUS, ERROR_STATUS):
                return self._error_response("Invalid response envelope")
            return response
        except (json.JSONDecodeError, TypeError) as exc:
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
            reply = self._replies.pop(req_id, None)
            deadline = time.monotonic() + self._timeo
            while reply is None:
                remaining = max(0, deadline - time.monotonic())
                candidate = self._broker_client.rep(timeo=remaining)
                if candidate is None:
                    break
                if candidate.get("req_id") == req_id:
                    reply = candidate
                    break
                self._replies[candidate["req_id"]] = candidate
                if time.monotonic() >= deadline:
                    break
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
            if self._replies:
                reply = self._replies.pop(next(iter(self._replies)))
            else:
                reply = self._broker_client.rep(timeo=self._timeo if timeo is None else timeo)

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

    def call(self, service, method, args=None, kwargs=None, close=False):
        """Return a structured response, preserving status and error details."""
        try:
            req_id = self.send(service, method, args, kwargs)
            if req_id is None:
                return self._error_response(self.no_req_msg)
            return self.receive(req_id)
        finally:
            if close:
                self.close()

    def invoke(self, service, method, args=None, kwargs=None, close=False,
               raise_errors=False):
        """Return output (or legacy error text); optionally raise RPCError."""
        response = self.call(service, method, args, kwargs, close)
        if raise_errors and response.get("status") != OK_STATUS:
            raise RPCError(response)
        return self.parse(response)

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
