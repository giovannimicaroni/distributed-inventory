import os
import sys
import time
import random
import threading
import logging
import grpc
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import inventory_pb2
import inventory_pb2_grpc

from lamport_clock import LamportClock
from mutex import RicartAgrawala
from election import BullyElection
from inventory import InventoryState

NODE_ID   = int(os.environ['NODE_ID'])
NODE_PORT = int(os.environ['NODE_PORT'])
PEERS     = [p for p in os.environ.get('PEERS', '').split(',') if p]

clock = LamportClock()
mutex = RicartAgrawala(NODE_ID, clock)
election = None  # initialised in main() after stubs are ready
inventory = InventoryState(initial=20)


def log(msg):
    print(f"[node{NODE_ID} ts={clock.value()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# gRPC servicer
# ---------------------------------------------------------------------------

class NodeServicer(inventory_pb2_grpc.NodeServiceServicer):
    def SendMessage(self, request, context):
        clock.update(request.lamport_ts)
        log(f"recv from node{request.sender_id} content='{request.content}'")
        return inventory_pb2.Message(
            sender_id=NODE_ID,
            lamport_ts=clock.tick(),
            content=f"ack from node{NODE_ID}",
        )

    def RequestCS(self, request, context):
        clock.update(request.lamport_ts)
        reply_ts = mutex.on_request(request.node_id, request.lamport_ts)
        return inventory_pb2.CSReply(node_id=NODE_ID, lamport_ts=reply_ts)

    def Election(self, request, context):
        clock.update(request.lamport_ts)
        election.on_election(request.node_id)
        return inventory_pb2.ElectionAck(
            node_id=NODE_ID, lamport_ts=clock.tick(), ok=True)

    def Coordinator(self, request, context):
        clock.update(request.lamport_ts)
        election.on_coordinator(request.leader_id)
        return inventory_pb2.CoordinatorAck(node_id=NODE_ID, lamport_ts=clock.tick())

    def Ping(self, request, context):
        clock.update(request.lamport_ts)
        return inventory_pb2.PingReply(node_id=NODE_ID, lamport_ts=clock.tick())

    def SyncInventory(self, request, context):
        clock.update(request.lamport_ts)
        inventory.sync(request.inventory)
        log(f"synced inventory={request.inventory} from node{request.sender_id}")
        return inventory_pb2.SyncAck(sender_id=NODE_ID, lamport_ts=clock.tick())


def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_NodeServiceServicer_to_server(NodeServicer(), server)
    server.add_insecure_port(f'0.0.0.0:{NODE_PORT}')
    server.start()
    log(f"server listening on port {NODE_PORT}")
    return server


# ---------------------------------------------------------------------------
# Peer stubs
# ---------------------------------------------------------------------------

def peer_id_from_addr(addr: str) -> int:
    # "node2:50052" → 2
    return int(addr.split(':')[0].replace('node', ''))


def make_stubs():
    return {addr: inventory_pb2_grpc.NodeServiceStub(grpc.insecure_channel(addr))
            for addr in PEERS}


def wait_for_peers(stubs: dict):
    log("waiting for peers...")
    for addr, stub in stubs.items():
        while True:
            try:
                stub.SendMessage(inventory_pb2.Message(
                    sender_id=NODE_ID,
                    lamport_ts=clock.tick(),
                    content="hello",
                ), timeout=2)
                log(f"peer {addr} is up")
                break
            except grpc.RpcError:
                time.sleep(1)


# ---------------------------------------------------------------------------
# Inventory broadcast helper
# ---------------------------------------------------------------------------

def _broadcast_sync(stubs: dict):
    count = inventory.count
    ts = clock.tick()
    for stub in stubs.values():
        try:
            stub.SyncInventory(inventory_pb2.SyncMsg(
                sender_id=NODE_ID, lamport_ts=ts, inventory=count), timeout=2)
        except grpc.RpcError:
            pass


# ---------------------------------------------------------------------------
# Checkout loop (exercises Ricart-Agrawala mutex)
# ---------------------------------------------------------------------------

def checkout_loop(stubs: dict):
    while True:
        time.sleep(random.uniform(2, 5))
        log("REQUESTING CS")
        mutex.request_cs(stubs)
        log(f"ENTERING CS — inventory={inventory.count}")
        success = inventory.checkout()
        if success:
            log(f"CHECKOUT OK — inventory now {inventory.count}")
            _broadcast_sync(stubs)
        else:
            log("CHECKOUT FAILED — inventory empty")
        log("EXITING CS")
        mutex.release_cs()


# ---------------------------------------------------------------------------
# Restock loop (leader only)
# ---------------------------------------------------------------------------

def restock_loop(stubs: dict):
    while True:
        time.sleep(10)
        if election and election.is_leader():
            inventory.restock(5)
            log(f"RESTOCK — inventory now {inventory.count}")
            _broadcast_sync(stubs)


# ---------------------------------------------------------------------------
# Heartbeat loop (detects leader failure and triggers re-election)
# ---------------------------------------------------------------------------

def heartbeat_loop(stubs_by_id: dict):
    while True:
        time.sleep(3)
        with election._lock:
            leader_id = election.leader
        if leader_id is None or leader_id == NODE_ID:
            continue
        stub = stubs_by_id.get(leader_id)
        if stub is None:
            threading.Thread(target=election.start_election, daemon=True).start()
            continue
        try:
            stub.Ping(
                inventory_pb2.PingRequest(node_id=NODE_ID, lamport_ts=clock.tick()),
                timeout=2,
            )
        except grpc.RpcError:
            log(f"leader node{leader_id} unreachable — triggering election")
            threading.Thread(target=election.start_election, daemon=True).start()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global election

    server = serve()

    stubs = make_stubs()
    wait_for_peers(stubs)

    stubs_by_id = {peer_id_from_addr(addr): stub for addr, stub in stubs.items()}
    election = BullyElection(NODE_ID, clock, stubs_by_id, log)

    threading.Thread(target=checkout_loop, args=(stubs,), daemon=True).start()
    threading.Thread(target=heartbeat_loop, args=(stubs_by_id,), daemon=True).start()
    threading.Thread(target=restock_loop, args=(stubs,), daemon=True).start()
    threading.Thread(target=election.start_election, daemon=True).start()

    server.wait_for_termination()


if __name__ == '__main__':
    main()
