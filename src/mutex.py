import threading
from enum import Enum
import sys
import os

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import inventory_pb2 


class State(Enum):
    RELEASED = "RELEASED"
    WANTED   = "WANTED"
    HELD     = "HELD"


class RicartAgrawala:
    def __init__(self, node_id, clock):
        self.node_id = node_id
        self.clock   = clock
        self.state      = State.RELEASED
        self.request_ts = None  # (ts, node_id) of our current request
        self.deferred   = []    # threading.Event per deferred incoming request
        self._lock = threading.Lock()

    def request_cs(self, stubs):
        """Broadcast RequestCS to all peers concurrently; block until all reply."""
        with self._lock:
            ts = self.clock.tick()
            self.request_ts = (ts, self.node_id)
            self.state = State.WANTED

        done_events = []
        for stub in stubs.values():
            ev = threading.Event()
            done_events.append(ev)
            threading.Thread(
                target=self._send_request, args=(stub, ts, ev), daemon=True
            ).start()

        for ev in done_events:
            ev.wait()

        with self._lock:
            self.state = State.HELD

    def _send_request(self, stub, ts, done_event):
        try:
            reply = stub.RequestCS(
                inventory_pb2.CSRequest(node_id=self.node_id, lamport_ts=ts),
                timeout=30,
            )
            self.clock.update(reply.lamport_ts)
        except grpc.RpcError:
            pass  # treat unreachable peer as having granted
        finally:
            done_event.set()

    def release_cs(self):
        """Release the CS; unblock all deferred servicer threads."""
        with self._lock:
            self.state = State.RELEASED
            events = self.deferred[:]
            self.deferred = []
        for ev in events:
            ev.set()

    def on_request(self, sender_id, req_ts):
        """
        Handle an incoming RequestCS. Blocks until it is safe to grant.
        Returns the Lamport timestamp to include in the CSReply.
        """
        grant_event = None
        with self._lock:
            # Defer if we are HELD, or if we are WANTED and have higher priority
            # (lower tuple wins — lower ts, then lower node_id)
            should_defer = (
                self.state == State.HELD or
                (self.state == State.WANTED and
                 self.request_ts is not None and
                 self.request_ts < (req_ts, sender_id))
            )
            if should_defer:
                grant_event = threading.Event()
                self.deferred.append(grant_event)

        if grant_event:
            grant_event.wait()

        return self.clock.tick()
