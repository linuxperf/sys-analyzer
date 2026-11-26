"""One-shot gRPC faults for the real Agent and Controller on loopback."""

import select
import socket
import socketserver
import struct
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto


class _Action(Enum):
    HOLD = auto()
    RESET = auto()
    HOLD_THEN_RESET = auto()


class Fault(Enum):
    # Register is the first RPC on a fresh Agent connection. Heartbeat and
    # Deregister follow it. Deregister fixtures send a signal before the
    # first scheduled heartbeat.
    HOLD_REGISTER_REPLY = (1, "reply", _Action.HOLD)
    RESET_REGISTER_REPLY = (1, "reply", _Action.RESET)
    HOLD_THEN_RESET_REGISTER_REPLY = (1, "reply", _Action.HOLD_THEN_RESET)
    RESET_HEARTBEAT_REPLY = (2, "reply", _Action.RESET)
    HOLD_DEREGISTER_REQUEST = (2, "request", _Action.HOLD)
    RESET_DEREGISTER_REQUEST = (2, "request", _Action.RESET)


@dataclass(frozen=True)
class FaultProxy:
    address: str
    intercepted: threading.Event
    _release: threading.Event

    def release(self):
        self._release.set()


_CLIENT_PREFACE_SIZE = 24
_FRAME_HEADER_SIZE = 9
_HEADERS = 1


@contextmanager
def start_fault_proxy(grpc_addr, fault, timeout):
    host, port = grpc_addr.rsplit(":", 1)
    upstream_addr = (host, int(port))
    intercepted = threading.Event()
    release = threading.Event()
    claim_lock = threading.Lock()
    rpc_number, direction, action = fault.value
    on_request = direction == "request"
    hold = action in (_Action.HOLD, _Action.HOLD_THEN_RESET)
    reset = action in (_Action.RESET, _Action.HOLD_THEN_RESET)

    class Relay(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                with socket.create_connection(upstream_addr, timeout=timeout) as upstream:
                    self.relay(self.request, upstream)
            except (ConnectionError, TimeoutError):
                pass

        def relay(self, downstream, upstream):
            # Observe request HEADERS to associate the fixture's RPC with the
            # actual stream, whatever stream IDs the HTTP/2 client chooses.
            buffers = {downstream: bytearray(), upstream: bytearray()}
            preface_left = _CLIENT_PREFACE_SIZE
            request_streams = set()
            target_stream = None
            while readable := select.select([downstream, upstream], [], [], timeout)[0]:
                for source in readable:
                    from_client = source is downstream
                    target = upstream if from_client else downstream
                    data = source.recv(65536)
                    if not data:
                        return
                    pending = buffers[source]
                    pending.extend(data)
                    if from_client and preface_left:
                        size = min(preface_left, len(pending))
                        target.sendall(pending[:size])
                        del pending[:size]
                        preface_left -= size
                    while len(pending) >= _FRAME_HEADER_SIZE:
                        end = _FRAME_HEADER_SIZE + int.from_bytes(pending[:3], "big")
                        if len(pending) < end:
                            break
                        if pending[3] == _HEADERS:
                            stream = int.from_bytes(pending[5:9], "big") & 0x7fffffff
                            if from_client and target_stream is None:
                                request_streams.add(stream)
                                if len(request_streams) == rpc_number:
                                    target_stream = stream
                            if from_client == on_request and stream == target_stream:
                                with claim_lock:
                                    inject = not intercepted.is_set()
                                    if inject:
                                        intercepted.set()
                                if inject:
                                    if hold and not release.wait(timeout):
                                        return
                                    if reset:
                                        downstream.setsockopt(
                                            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                                        )
                                        downstream.close()
                                        return
                        target.sendall(pending[:end])
                        del pending[:end]

    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), Relay) as server:
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            yield FaultProxy(f"{host}:{port}", intercepted, release)
        finally:
            release.set()
            server.shutdown()
            thread.join()
