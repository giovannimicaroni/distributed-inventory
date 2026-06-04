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

NODE_ID   = int(os.environ['NODE_ID'])
NODE_PORT = int(os.environ['NODE_PORT'])
PEERS     = [p for p in os.environ.get('PEERS', '').split(',') if p]

clock = LamportClock()


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
# Lamport clock test loop
# ---------------------------------------------------------------------------

def lamport_test_loop(stubs: dict):
    peer_list = list(stubs.items())
    while True:
        time.sleep(random.uniform(1, 3))
        addr, stub = random.choice(peer_list)
        ts = clock.tick()
        log(f"send to {addr} ts={ts}")
        try:
            reply = stub.SendMessage(inventory_pb2.Message(
                sender_id=NODE_ID,
                lamport_ts=ts,
                content=f"ping from node{NODE_ID}",
            ), timeout=5)
            clock.update(reply.lamport_ts)
            log(f"ack from {addr} reply_ts={reply.lamport_ts}")
        except grpc.RpcError as e:
            log(f"RPC to {addr} failed: {e.code()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    server = serve()

    stubs = make_stubs()
    wait_for_peers(stubs)

    threading.Thread(target=lamport_test_loop, args=(stubs,), daemon=True).start()

    server.wait_for_termination()


if __name__ == '__main__':
    main()
