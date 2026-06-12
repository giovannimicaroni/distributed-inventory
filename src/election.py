import threading
import sys
import os

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import inventory_pb2

ELECTION_TIMEOUT = 3  # seconds before declaring self as leader


class BullyElection:
    def __init__(self, node_id, clock, peers_by_id, log_fn):
        # peers_by_id: {peer_id: stub} for all peers
        self.node_id = node_id
        self.clock = clock
        self.peers_by_id = peers_by_id
        self._log = log_fn
        self.leader = None
        self._lock = threading.Lock()
        self._got_ack = threading.Event()
        self._in_election = False

    def start_election(self):
        with self._lock:
            if self._in_election:
                return
            self._in_election = True
            self._got_ack.clear()

        self._log("ELECTION started")

        higher_peers = {pid: stub for pid, stub in self.peers_by_id.items()
                        if pid > self.node_id}

        for pid, stub in higher_peers.items():
            threading.Thread(
                target=self._send_election, args=(pid, stub), daemon=True
            ).start()

        got_response = self._got_ack.wait(timeout=ELECTION_TIMEOUT)

        if not got_response:
            self._become_leader()
        else:
            with self._lock:
                self._in_election = False

    def _send_election(self, peer_id, stub):
        try:
            reply = stub.Election(
                inventory_pb2.ElectionMsg(
                    node_id=self.node_id,
                    lamport_ts=self.clock.tick(),
                ),
                timeout=ELECTION_TIMEOUT,
            )
            self.clock.update(reply.lamport_ts)
            if reply.ok:
                self._got_ack.set()
        except grpc.RpcError:
            pass

    def _become_leader(self):
        with self._lock:
            self.leader = self.node_id
            self._in_election = False
        self._log(f"COORDINATOR node{self.node_id} is the new leader")
        for stub in self.peers_by_id.values():
            threading.Thread(
                target=self._send_coordinator, args=(stub,), daemon=True
            ).start()

    def _send_coordinator(self, stub):
        try:
            reply = stub.Coordinator(
                inventory_pb2.CoordinatorMsg(
                    leader_id=self.node_id,
                    lamport_ts=self.clock.tick(),
                ),
                timeout=ELECTION_TIMEOUT,
            )
            self.clock.update(reply.lamport_ts)
        except grpc.RpcError:
            pass

    def on_election(self, sender_id):
        if sender_id < self.node_id:
            threading.Thread(target=self.start_election, daemon=True).start()

    def on_coordinator(self, leader_id):
        with self._lock:
            self.leader = leader_id
            self._in_election = False
        self._log(f"COORDINATOR accepted: node{leader_id} is the leader")

    def is_leader(self):
        return self.leader == self.node_id
