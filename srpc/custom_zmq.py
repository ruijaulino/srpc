import datetime as dt
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple, Union

import zmq


# -----------------------------------------------------------------------------
# Protocol constants
# -----------------------------------------------------------------------------

ENCODING = "utf-8"

COMM_HEARTBEAT_INTERVAL = 5
COMM_WORKER_EXPIRY_FACTOR = 2.5

COMM_HEADER_WORKER = "W"
COMM_HEADER_CLIENT = "C"

COMM_TYPE_HEARTBEAT = "H"
COMM_TYPE_REP = "REP"
COMM_TYPE_REQ = "REQ"
COMM_TYPE_ERROR = "ERR"

COMM_MSG_HEARTBEAT = ""

DEFAULT_SOCKET_TIMEOUT = 1.0
DEFAULT_REQUEST_TTL = 60.0
DEFAULT_MAX_PENDING_REQUESTS = 10_000


 

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def create_identity(prefix: str = "") -> str:
    suffix = uuid.uuid4().hex[:12].upper()
    return f"{prefix}{suffix}" if prefix else suffix


def ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def setup_basic_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def encode_frame(value: Union[str, bytes]) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode(ENCODING)


def decode_frame(value: bytes) -> str:
    return value.decode(ENCODING, errors="replace")


def encode_msg(msg: Union[str, bytes, List[Union[str, bytes]]]) -> Union[bytes, List[bytes]]:
    if isinstance(msg, list):
        return [encode_frame(e) for e in msg]
    return encode_frame(msg)


def decode_msg(msg: Union[bytes, List[bytes]]) -> Union[str, List[str]]:
    if isinstance(msg, list):
        return [decode_frame(e) if isinstance(e, bytes) else str(e) for e in msg]
    return decode_frame(msg) if isinstance(msg, bytes) else str(msg)


# -----------------------------------------------------------------------------
# Generic ZMQ wrapper
# -----------------------------------------------------------------------------

class ZMQPub:
    def __init__(self, ctx:zmq.Context = None, timeo:int = 1):
        self.ctx = ctx
        self.timeo = 1000*timeo if timeo else 1000        
        self.addr = None
        self.socket = None

    def connect(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.addr)
        # time.sleep(1) # make sure all connections have time to be established
    
    def publish(self, topic:str, msg:str):        
        if (self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT) != 0:            
            self.socket.send_multipart([topic.encode(), msg.encode()])
            
    def close(self):
        self.socket.close()


class ZMQSub:
    def __init__(self, ctx:zmq.Context = None, last_msg_only:bool = False, timeo:int = 1):
        self.ctx = ctx
        self.conflate = 1 if last_msg_only else 0
        self.timeo = 1000*timeo if timeo else 1000        
        self.addr = None
        self.socket = None

    def connect(self, addr:str = None):
        self.addr = addr
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.CONFLATE, self.conflate) 
        self.socket.connect(self.addr)
        # time.sleep(1) # make sure all connections have time to be established

    def unsubscribe(self, topic:str):
        self.socket.unsubscribe(topic.encode())

    def subscribe(self, topic:str = ''):
        self.socket.subscribe(topic.encode())
    
    def recv(self):
        # if we get a KeyboardInterrupt (or other error...) while listening we return a topic = -1
        try:
            if (self.socket.poll(self.timeo) & zmq.POLLIN) != 0:
                topic, msg = self.socket.recv_multipart()
                return topic.decode(), msg.decode()
        except:
            return -1, None
        return None, None
    
    def close(self):
        self.socket.close()




# proxy with last value caching
def ZMQProxy(pub_addr:str, sub_addr:str, topics_no_cache = ['trigger'], stop_event:threading.Event = None):


    print(f'[{ts()}] Starting Proxy from {sub_addr} to {pub_addr}')

    stop_event = stop_event if stop_event else threading.Event()

    ctx = zmq.Context.instance()

    xsub_socket = ctx.socket(zmq.XSUB) 
    xsub_socket.bind(sub_addr)
    xsub_socket.setsockopt(zmq.LINGER, 0)

    xpub_socket = ctx.socket(zmq.XPUB) 
    xpub_socket.bind(pub_addr)
    xpub_socket.setsockopt(zmq.LINGER, 0)
    # this flag is needed otherwise the xpub will not receive repeated topics sub request
    # and we cannot reroute the last message
    xpub_socket.setsockopt(zmq.XPUB_VERBOSE, True) 

    # poller for sockets
    poller = zmq.Poller()
    poller.register(xsub_socket, zmq.POLLIN)
    poller.register(xpub_socket, zmq.POLLIN)   

    # cache
    cache = {}
    # make the xsub receive all messages
    xsub_socket.send_multipart([b'\x01'])

    while not stop_event.isSet():            
        try:
            # poll for events
            events = dict(poller.poll(1000))
            # for any new topic data, cache it and then forward
            if xsub_socket in events:
                # get msg
                tmp = xsub_socket.recv_multipart()
                # update cache 
                topic, msg = tmp                
                dec_topic = topic.decode()
                c = True
                for t in topics_no_cache:
                    if dec_topic.startswith(t):
                        c = False
                        break
                if c:
                    cache[dec_topic] = msg.decode()
                # publish back
                xpub_socket.send_multipart(tmp)

            # handle subscriptions
            # When we get a new subscription we pull data from the cache:
            if xpub_socket in events:                
                msg = xpub_socket.recv()
                # Event is one byte 0=unsub or 1=sub, followed by topic            
                # print(cache)
                if msg[0] == 1:
                    st = msg[1:].decode()
                    # need to run all that startswith
                    for k,v in cache.items():
                        if k.startswith(st):
                            xpub_socket.send_multipart([k.encode(), v.encode()])
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f'[{ts()}] Error in proxy: ', e)
            break

    print(f'[{ts()}] Terminating Proxy')
    
    # close sockets
    xsub_socket.close()
    xpub_socket.close()

    # terminate context
    ctx.destroy()




class ZMQR:
    """Small ZMQ socket wrapper with timeouts and explicit reconnect.

    This wrapper intentionally keeps the API close to your original version.
    """

    def __init__(
        self,
        ctx: zmq.Context,
        zmq_type: int,
        timeo: float = DEFAULT_SOCKET_TIMEOUT,
        identity: Optional[str] = None,
        reconnect: bool = True,
    ):
        assert zmq_type in [zmq.REQ, zmq.REP, zmq.DEALER], "ZMQR must use REQ, REP, or DEALER"
        self.timeo_ms = int(1000 * timeo)
        self.ctx = ctx
        self.socket: Optional[zmq.Socket] = None
        self.identity = identity or create_identity()
        self.zmq_type = zmq_type
        self.addr: List[str] = []
        self.bind_addr = ""
        self.binded = False
        self.reconnect = reconnect

    def set_identity(self, identity: Optional[str] = None) -> None:
        self.identity = identity or create_identity()
        if self.socket and self.identity:
            self.socket.setsockopt_string(zmq.IDENTITY, self.identity)

    def _build_socket(self) -> None:
        if self.socket is not None:
            self.close()
        self.socket = self.ctx.socket(self.zmq_type)
        if self.identity:
            self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.setsockopt(zmq.LINGER, 0)

    def bind(self, addr: Optional[str] = None) -> None:
        self.bind_addr = addr or self.bind_addr
        if not self.bind_addr:
            raise ValueError("bind address is empty")
        self.binded = True
        self._build_socket()
        assert self.socket is not None
        self.socket.bind(self.bind_addr)

    def _connect(self, addr: Optional[str] = None) -> None:
        if self.binded:
            raise RuntimeError("trying to connect a socket that was bound")
        if addr and addr not in self.addr:
            self.addr.append(addr)
        if not self.addr:
            raise ValueError("connect address list is empty")
        self._build_socket()
        assert self.socket is not None
        for item in self.addr:
            self.socket.connect(item)

    def connect(self, addr: Optional[str] = None) -> None:
        self._connect(addr)

    def _reconnect(self) -> None:
        if not self.reconnect:
            return
        if self.binded:
            self.bind()
        else:
            self.connect()

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

    def send(self, msg: Union[str, bytes], timeo: Optional[float] = None) -> bool:
        if self.socket is None:
            raise RuntimeError("socket is not connected/bound")
        timeout_ms = int(1000 * timeo) if timeo is not None else self.timeo_ms
        if self.socket.poll(timeout_ms, zmq.POLLOUT) & zmq.POLLOUT:
            self.socket.send(encode_msg(msg))
            return True
        if self.zmq_type == zmq.REP:
            self._reconnect()
        return False

    def send_multipart(self, msg: List[Union[str, bytes]], timeo: Optional[float] = None) -> bool:
        if self.socket is None:
            raise RuntimeError("socket is not connected/bound")
        timeout_ms = int(1000 * timeo) if timeo is not None else self.timeo_ms
        if self.socket.poll(timeout_ms, zmq.POLLOUT) & zmq.POLLOUT:
            self.socket.send_multipart(encode_msg(msg))
            return True
        if self.zmq_type == zmq.REP:
            self._reconnect()
        return False

    def recv(self, timeo: Optional[float] = None) -> Optional[str]:
        if self.socket is None:
            raise RuntimeError("socket is not connected/bound")
        timeout_ms = int(1000 * timeo) if timeo is not None else self.timeo_ms
        if self.socket.poll(timeout_ms, zmq.POLLIN) & zmq.POLLIN:
            return decode_msg(self.socket.recv())  # type: ignore[return-value]
        if self.zmq_type in [zmq.REQ, zmq.DEALER]:
            self._reconnect()
        return None

    def recv_multipart(self, timeo: Optional[float] = None) -> Optional[List[str]]:
        if self.socket is None:
            raise RuntimeError("socket is not connected/bound")
        timeout_ms = int(1000 * timeo) if timeo is not None else self.timeo_ms
        if self.socket.poll(timeout_ms, zmq.POLLIN) & zmq.POLLIN:
            return decode_msg(self.socket.recv_multipart())  # type: ignore[return-value]
        if self.zmq_type in [zmq.REQ, zmq.DEALER]:
            self._reconnect()
        return None


# -----------------------------------------------------------------------------
# Broker internals
# -----------------------------------------------------------------------------

@dataclass
class PendingRequest:
    client_id: str
    payload: str
    req_id: str
    expiry: float


@dataclass
class InFlightRequest:
    worker_id: str
    client_id: str
    req_id: str
    payload: str
    expiry: float


class ZMQServiceBrokerService:
    """State for one service name."""

    def __init__(
        self,
        name: str,
        request_ttl: float = DEFAULT_REQUEST_TTL,
        max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS,
    ):
        self.name = name
        self.request_ttl = request_ttl
        self.max_pending_requests = max_pending_requests

        self.requests: Deque[PendingRequest] = deque()
        self.ready_workers: Deque[str] = deque()
        self.worker_expiry: Dict[str, float] = {}
        self.known_workers = set()
        self.inflight_by_worker: Dict[str, InFlightRequest] = {}

    def add_request(self, client_id: str, req_id: str, payload: str) -> Optional[str]:
        if len(self.requests) >= self.max_pending_requests:
            return f"service {self.name!r} queue is full"
        self.requests.append(
            PendingRequest(
                client_id=client_id,
                req_id=req_id,
                payload=payload,
                expiry=time.time() + self.request_ttl,
            )
        )
        return None

    def add_worker(self, worker_id: str) -> None:
        if worker_id not in self.known_workers:
            print(f"New worker {worker_id} for service {self.name}")
            self.known_workers.add(worker_id)

        self.worker_expiry[worker_id] = time.time() + COMM_HEARTBEAT_INTERVAL*COMM_WORKER_EXPIRY_FACTOR

        # A worker that contacts us is ready again. Remove stale duplicate readiness.
        try:
            self.ready_workers.remove(worker_id)
        except ValueError:
            pass
        self.ready_workers.append(worker_id)

        # If the worker replied or heartbeated after an in-flight timeout, clear stale state.
        self.inflight_by_worker.pop(worker_id, None)

    def mark_worker_busy(self, worker_id: str, req: PendingRequest) -> None:
        self.inflight_by_worker[worker_id] = InFlightRequest(
            worker_id=worker_id,
            client_id=req.client_id,
            req_id=req.req_id,
            payload=req.payload,
            expiry=time.time() + self.request_ttl,
        )

    def next_worker(self) -> str:
        return self.ready_workers.popleft()

    def next_request(self) -> PendingRequest:
        return self.requests.popleft()

    def has_workers_and_requests(self) -> bool:
        return bool(self.ready_workers) and bool(self.requests)

    def has_workers(self) -> bool:
        return bool(self.ready_workers or self.worker_expiry or self.inflight_by_worker)

    def purge_expired_requests(self) -> List[Tuple[str, str]]:
        """Drop expired queued requests and return client errors to send."""
        now = time.time()
        kept: Deque[PendingRequest] = deque()
        expired: List[Tuple[str, str]] = []

        while self.requests:
            req = self.requests.popleft()
            if now <= req.expiry:
                kept.append(req)
            else:
                expired.append((req.client_id, req.req_id, f"request to service {self.name!r} expired before dispatch"))

        self.requests = kept
        return expired

    def purge_expired_workers(self) -> List[PendingRequest]:
        """Remove dead workers DO NOT requeue their in-flight work, if any."""
        now = time.time()
        requeue: List[PendingRequest] = []
        expired_workers = [worker_id for worker_id, expiry in self.worker_expiry.items() if now > expiry]


        for worker_id in expired_workers:
            if worker_id not in self.inflight_by_worker:
                print(f"Worker {worker_id} for service {self.name} expired")
                self.worker_expiry.pop(worker_id, None)
                self.known_workers.discard(worker_id)
                try:
                    self.ready_workers.remove(worker_id)
                except ValueError:
                    pass

                inflight = self.inflight_by_worker.pop(worker_id, None)
                #if inflight and now <= inflight.expiry:
                #    requeue.append(
                #        PendingRequest(
                #            client_id=inflight.client_id,
                #            payload=inflight.payload,
                #            req_id=inflight.req_id,
                #            expiry=inflight.expiry,
                #        )
                #    )

        #for req in reversed(requeue):
        #    self.requests.appendleft(req)

        return requeue

    def purge_expired_inflight(self) -> List[Tuple[str, str]]:
        """Fail requests that were sent to a worker but never got a reply."""
        now = time.time()
        failed: List[Tuple[str, str]] = []
        expired_workers = [
            worker_id for worker_id, req in self.inflight_by_worker.items() if now > req.expiry
        ]
        for worker_id in expired_workers:
            req = self.inflight_by_worker.pop(worker_id)
            failed.append((req.client_id, req.req_id, f"request to service {self.name!r} timed out while processing"))
        return failed

    def purge(self) -> List[Tuple[str, str]]:
        errors = []
        errors.extend(self.purge_expired_requests())
        self.purge_expired_workers()
        errors.extend(self.purge_expired_inflight())
        return errors


# -----------------------------------------------------------------------------
# Broker
# -----------------------------------------------------------------------------

class ZMQServiceBroker:
    """ROUTER broker that routes client requests to service workers."""

    def __init__(
        self,
        addr: str,
        timeo: int = 1000,
        stop_event: Optional[threading.Event] = None,
        request_ttl: float = DEFAULT_REQUEST_TTL,
        max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS,
    ):
        self.addr = addr
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.bind(addr)

        self.timeo = timeo
        self.stop_event = stop_event or threading.Event()
        self.request_ttl = request_ttl
        self.max_pending_requests = max_pending_requests
        self.services: Dict[str, ZMQServiceBrokerService] = {}

    def _decode(self, msg: List[bytes]) -> List[str]:
        return [decode_frame(e) for e in msg]

    def _encode(self, msg: List[Union[str, bytes]]) -> List[bytes]:
        return [encode_frame(e) for e in msg]

    def recv(self) -> Optional[List[str]]:
        if self.socket.poll(self.timeo, zmq.POLLIN) & zmq.POLLIN:
            return self._decode(self.socket.recv_multipart())
        return None

    def send(self, msg: List[Union[str, bytes]]) -> bool:
        if self.socket.poll(self.timeo, zmq.POLLOUT) & zmq.POLLOUT:
            self.socket.send_multipart(self._encode(msg))
            return True
        print(f"Broker send timeout for message with first frames: {msg[:3]}")
        return False

    def send_client_reply(self, client_id: str, req_id:str, payload: str) -> None:
        self.send([client_id, req_id, payload])

    def send_client_error(self, client_id: str, req_id:str, error: str) -> None:
        self.send([client_id, req_id, f"{COMM_TYPE_ERROR}:{error}"])

    def require_service(self, service_name: str) -> ZMQServiceBrokerService:
        if service_name not in self.services:
            self.services[service_name] = ZMQServiceBrokerService(
                service_name,
                request_ttl=self.request_ttl,
                max_pending_requests=self.max_pending_requests,
            )
        return self.services[service_name]

    def dispatch(self, service: ZMQServiceBrokerService) -> None:
        while service.has_workers_and_requests():
            req = service.next_request()
            worker_id = service.next_worker()
            service.mark_worker_busy(worker_id, req)
            ok = self.send([worker_id, COMM_TYPE_REQ, req.client_id, req.req_id, req.payload])
            #if not ok:
            #    # If we could not send, put the request back.
            #    service.requests.appendleft(req)
            #    service.inflight_by_worker.pop(worker_id, None)
            #    break

    def purge(self) -> None:
        empty_services = []
        for service_name, service in self.services.items():
            errors = service.purge()
            for client_id, req_id, error in errors:
                print(client_id, req_id, error)
                self.send_client_error(client_id, req_id, error)
            if not service.has_workers() and not service.requests:
                empty_services.append(service_name)

        for service_name in empty_services:
            self.services.pop(service_name, None)

    def handle_client_message(self, sender: str, frames: List[str]) -> None:
        # Expected after header: [service, payload]
        if len(frames) != 3:
            self.send_client_error(sender, 'UNK', "invalid client message format")
            return

        service_name, reqid, payload = frames
        
        service = self.require_service(service_name)
        error = service.add_request(sender, reqid, payload)
        if error:
            self.send_client_error(sender, 'UNK', error)
            return
        self.dispatch(service)

    def handle_worker_message(self, sender: str, frames: List[str]) -> None:
        # Expected after header: [comm_type, service, ...]
        if len(frames) < 2:
            print(f"Invalid worker message from {sender}: {frames}")
            return

        comm_type = frames.pop(0)
        service_name = frames.pop(0)
        service = self.require_service(service_name)
        service.add_worker(sender)

        if comm_type == COMM_TYPE_HEARTBEAT:
            self.send([sender, COMM_TYPE_HEARTBEAT, COMM_MSG_HEARTBEAT])

        elif comm_type == COMM_TYPE_REP:
            # Expected remaining frames: [client_id, payload]
            if len(frames) != 3:
                print(f"Wrong reply format from worker {sender}: {frames}")
            else:
                client_id, req_id, payload = frames
                self.send_client_reply(client_id, req_id, payload)

        else:
            print(f"Unknown worker comm_type from {sender}: {comm_type}")

        self.dispatch(service)

    def handle_message(self, msg: List[str]) -> None:
        # ROUTER receives: [sender, '', header, ...] from REQ clients.
        # DEALER workers send: [sender, '', header, ...] if they include leading ''.
        # DEALER can also send [sender, header, ...], so normalize optional empty frame.
        if len(msg) < 3:
            print(f"Invalid short message: {msg}")
            return

        sender = msg.pop(0)
        if msg and msg[0] == "":
            msg.pop(0)

        if not msg:
            print(f"Invalid empty message body from {sender}", )
            return

        header = msg.pop(0)
        if header == COMM_HEADER_CLIENT:
            self.handle_client_message(sender, msg)
        elif header == COMM_HEADER_WORKER:
            self.handle_worker_message(sender, msg)
        else:
            print(f"Invalid message header from {sender}: {header}")

    def serve(self) -> None:
        print(f"Starting ZMQServiceBroker on {self.addr}")
        try:
            while not self.stop_event.is_set():
                try:
                    msg = self.recv()
                    if msg:
                        self.handle_message(msg)
                    self.purge()
                except KeyboardInterrupt:
                    print("ZMQServiceBroker KeyboardInterrupt")
                    break
                except zmq.ZMQError:
                    print("ZMQ error in broker loop")
                    # Keep serving unless the context/socket has been closed by shutdown.
                    if self.stop_event.is_set():
                        break
                except Exception as e:
                    # Production hardening: log bad input/state, but do not kill the broker.
                    print("Unexpected broker error: ", e)
        finally:
            print(f"Terminating ZMQServiceBroker on {self.addr}")
            self.socket.close(linger=0)
            self.ctx.term()
            print(f"ZMQServiceBroker on {self.addr} terminated")


# -----------------------------------------------------------------------------
# Worker
# -----------------------------------------------------------------------------

class ZMQServiceBrokerWorker(ZMQR):
    def __init__(self, ctx: zmq.Context, service: str, worker_info: bool = False):
        super().__init__(
            ctx=ctx,
            zmq_type=zmq.DEALER,
            timeo=COMM_HEARTBEAT_INTERVAL / 10,
            identity=create_identity("W-"),
            reconnect=False,
        )
        self.service = service
        self.worker_info = worker_info
        self.ping_t = 0.0
        self.pong_t = 0.0
        self.queue_alive = False

    def connect(self, addr: Optional[str] = None) -> None:
        self.set_identity(create_identity("W-"))
        self._connect(addr)
        if self.worker_info:
            print(f"Worker {self.identity} for service {self.service} ready")
        now = time.time()
        self.ping_t = now
        self.pong_t = now
        self.queue_alive = False
        self.send_heartbeat()

    def send_heartbeat(self) -> bool:
        return self.send_multipart([
            "",
            COMM_HEADER_WORKER,
            COMM_TYPE_HEARTBEAT,
            self.service,
            COMM_MSG_HEARTBEAT,
        ])

    def recv_work(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Receive one unit of work.

        Returns:
            (client_id, payload), or (None, None) if only heartbeat/no message.
        """
        msg = self.recv_multipart()
        client_id = None
        req_id = None
        payload = None

        if msg:
            self.queue_alive = True
            self.pong_t = time.time()

            comm_type = msg.pop(0)
            if comm_type == COMM_TYPE_REQ and len(msg) == 3:
                client_id, req_id, payload = msg[0], msg[1], msg[2]

            elif comm_type == COMM_TYPE_HEARTBEAT:
                pass
            else:
                print(f"Unknown message from queue to worker {self.identity}: {comm_type} {msg}")

        now = time.time()
        if not self.queue_alive and now - self.pong_t > COMM_HEARTBEAT_INTERVAL:
            print(f"Queue not responding to worker {self.identity}. Reconnecting...")
            self.connect()
        elif now > self.ping_t + COMM_HEARTBEAT_INTERVAL:
            self.ping_t = now
            self.queue_alive = False
            self.send_heartbeat()

        return client_id, req_id, payload

    def send_work(self, client_id: str, req_id:str, msg: str) -> bool:
        return self.send_multipart([
            "",
            COMM_HEADER_WORKER,
            COMM_TYPE_REP,
            self.service,
            client_id,
            req_id,
            msg,
        ])


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------

# class ZMQServiceBrokerClient(ZMQR):
#     def __init__(self, ctx: zmq.Context, info: bool = True):
#         super().__init__(
#             ctx=ctx,
#             zmq_type=zmq.DEALER, # REQ
#             timeo=COMM_HEARTBEAT_INTERVAL / 10,
#             identity=create_identity("C-"),
#             reconnect=True,
#         )
#         self.info = info

#     def connect(self, addr: Optional[str] = None) -> None:
#         self.set_identity(create_identity("C-"))
#         self._connect(addr)
#         if self.info:
#             print(f"Client {self.identity} ready", self.identity)

#     def req(self, service: str, msg: str, timeo: Optional[float] = None) -> bool:
#         reqid = create_identity("R-")
#         out = self.send_multipart([COMM_HEADER_CLIENT, service, reqid, msg], timeo=timeo)
#         if out: return reqid
#         return False


#     def rep(self, timeo: Optional[float] = None) -> Optional[dict]:
#         # reply with req_id, response
#         out = self.recv_multipart(timeo=timeo) # 
#         if isinstance(out, list):
#             if len(out) == 2:
#                 return {'req_id':out[0], 'rep':out[1]}
#         return {'req_id':None, 'rep':None}

#     def request(self, service: str, msg: str, timeo: Optional[float] = None) -> Optional[str]:
#         status = self.req(service, msg, timeo=timeo)
#         if not status:
#             print("Could not send request to service queue")
#             return None
#         return self.rep(timeo=timeo)

class ZMQServiceBrokerClient(ZMQR):
    def __init__(self, ctx: zmq.Context, info: bool = True):
        super().__init__(
            ctx=ctx,
            zmq_type=zmq.DEALER,
            timeo=COMM_HEARTBEAT_INTERVAL / 10,
            identity=create_identity("C-"),
            reconnect=True,
        )
        self.info = info

    def connect(self, addr: Optional[str] = None) -> None:
        self.set_identity(create_identity("C-"))
        self._connect(addr)

        if self.info:
            print(f"Client {self.identity} ready")

    def req(
        self,
        service: str,
        msg: str,
        timeo: Optional[float] = None,
    ) -> Optional[str]:
        """
        Send a request to a service.

        Returns the request id if sent successfully, otherwise None.
        """
        req_id = create_identity("R-")

        ok = self.send_multipart(
            [COMM_HEADER_CLIENT, service, req_id, msg],
            timeo=timeo,
        )

        return req_id if ok else None

    def rep(self, timeo: Optional[float] = None) -> Optional[dict]:
        """
        Receive a reply from the broker.

        Expected reply shape:
            [req_id, response]

        Returns:
            {"req_id": req_id, "rep": response}

        Or None if no valid reply was received.
        """
        out = self.recv_multipart(timeo=timeo)

        if not isinstance(out, list) or len(out) != 2:
            return None

        req_id, rep = out

        return {
            "req_id": req_id,
            "rep": rep,
        }

    def request(
        self,
        service: str,
        msg: str,
        timeo: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Send a request and wait for one reply.
        """
        req_id = self.req(service, msg, timeo=timeo)

        if req_id is None:
            if self.info:
                print("Could not send request to service queue")
            return None

        rep = self.rep(timeo=timeo)

        if rep is None:
            return None

        if rep.get("req_id") != req_id:
            if self.info:
                print("Received reply for a different request id")
            return None

        return rep