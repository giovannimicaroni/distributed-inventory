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

NODE_ID   = int(os.environ['NODE_ID'])
NODE_PORT = int(os.environ['NODE_PORT'])
PEERS     = [p for p in os.environ.get('PEERS', '').split(',') if p]

clock = LamportClock()
mutex = RicartAgrawala(NODE_ID, clock)


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
# Checkout loop (exercises Ricart-Agrawala mutex)
# ---------------------------------------------------------------------------

def checkout_loop(stubs: dict):
    while True:
        time.sleep(random.uniform(2, 5))
        log("REQUESTING CS")
        mutex.request_cs(stubs)
        log("ENTERING CS — inventory checkout")
        time.sleep(random.uniform(0.1, 0.5))
        log("EXITING CS")
        mutex.release_cs()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    server = serve()

    stubs = make_stubs()
    wait_for_peers(stubs)

    threading.Thread(target=checkout_loop, args=(stubs,), daemon=True).start()

    server.wait_for_termination()


if __name__ == '__main__':
    main()
