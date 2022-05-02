import asyncio
import json
import random
import sys
import traceback

from dataclasses import dataclass
from datetime import datetime

from dataclass_wizard import asdict, fromdict


EPOCH = datetime.fromtimestamp(0)
LOOP: asyncio.AbstractEventLoop


@dataclass
class RequestVote:
    term: int
    candidate_addr: str
    last_log_index: int
    last_log_term: int


@dataclass
class AppendEntries:
    term: int

    leader_addr: str
    leader_commit_index: int

    entries: list

    # For validation
    prev_log_index: int
    prev_log_term: int


@dataclass
class Log:
    index: int
    term: int


PONG = b'{"ok":true,"result":"PONG"}'
FAILURE = b'{"ok":false}'


class Peer(object):
    def __init__(self, addr: str):
        self.addr = addr
        self.host, self.port = addr.split(":")

        self.reader = None
        self.writer = None
        self.last_active_time = EPOCH

    async def initialize(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.last_active_time = datetime.now()

            return True
        except Exception:
            print(f"Failed to connect to peer at {self.addr}")
            traceback.print_exc()

            return False

    def is_connected(self):
        return self.reader is not None and self.writer is not None

    async def send(self, message: [bytes, str]) -> bytes:
        if isinstance(message, str):
            message = message.encode()

        self.writer.write(message)
        self.writer.write(b"\r\n")
        await self.writer.drain()

        return await self.reader.readline()

    async def ping(self):
        try:
            if self.reader is not None:
                response = await self.send(b"PING")
                if response == PONG:
                    self.last_active_time = datetime.now()
                    return True
            else:
                return await self.initialize()
        except Exception:
            print(f"Failed to ping peer at {self.addr}")
            traceback.print_exc()

        return False

    async def look_for_leader(self):
        if not self.is_connected():
            return None

        try:
            response = await self.send(b"LOOK_FOR_LEADER")
            if response and response != FAILURE:
                obj = json.loads(response)
                if obj.get("ok") is True:
                    return obj.get("value")
        except Exception:
            print(f"Failed to query peer at {self.addr} for Leader")
            traceback.print_exc()

        return None

    async def request_vote(self, rpc: RequestVote):
        if not self.is_connected():
            return False

        try:
            response = await self.send(f"REQUEST_VOTE {json.dumps(asdict(rpc))}")
            if response and response != FAILURE:
                obj = json.loads(response)
                if obj.get("ok") is True and obj.get("term") == rpc.term:
                    return True
        except Exception:
            print(f"Failed to send RequestVote RPC to peer at {self.addr}")
            traceback.print_exc()

        return False


class Node(object):
    def __init__(self, addr: str, peers: list[str]):
        self.addr = addr

        self.logs = [Log(0, 0)]
        self.commit_index = 0
        self.last_applied = 0

        self.current_term = 0

        self.voted_for = self.addr
        self.vote_timeout = 0.5 * random.uniform(1.0, 1.2)
        self.voting = False

        self.leader_addr = None

        # Only for Leader
        self.next_indices = {}
        self.match_indices = {}

        # Cluster status
        self.peers = [Peer(addr) for addr in peers]

    async def post_initialize(self):
        results = await asyncio.gather(*[Node.initialize_peer(peer) for peer in self.peers], return_exceptions=True)
        for index, result in enumerate(results):
            connected, leader_addr = result
            if connected is True:
                print(f"Connected to peer at {self.peers[index].addr}")
                if leader_addr is not None:
                    if self.leader_addr is not None and self.leader_addr != leader_addr:
                        if self.leader_addr != leader_addr:
                            raise RuntimeError("More than one Leader in the cluster!")
                    else:
                        print(f"Get Leader at {leader_addr}")
                    self.leader_addr = leader_addr

    @staticmethod
    async def initialize_peer(peer: Peer):
        result = await peer.initialize()
        if result is True:
            return True, await peer.look_for_leader()
        else:
            return False, None

    async def leader_send_heartbeat(self):
        results = await asyncio.gather(*[peer.ping() for peer in self.peers], return_exceptions=True)
        for index, result in enumerate(results):
            if not result:
                print(f"Can't ping peer at {self.peers[index].addr}")

    async def request_votes(self):
        while True:
            await asyncio.sleep(self.vote_timeout)

            if self.leader_addr is not None:
                continue

            quorum = (len(self.peers) + 1) // 2 + 1
            num_connected_nodes = 1
            for peer in self.peers:
                if peer.is_connected():
                    num_connected_nodes += 1
            if num_connected_nodes < quorum:
                continue

            self.voting = True
            self.current_term += 1
            print("Start voting for new term", self.current_term)

            rpc = RequestVote(self.current_term, self.addr, self.logs[-1].index, self.logs[-1].term)

            num_votes = 1
            results = await asyncio.gather(*[peer.request_vote(rpc) for peer in self.peers], return_exceptions=True)
            for index, result in enumerate(results):
                print(f"Peer at {self.peers[index].addr} {'voted for me' if result else 'NOT vote for me'}")
                if result:
                    num_votes += 1

            print(f"Cluster size: {len(self.peers) + 1}, quorum: {quorum}, votes: {num_votes}")
            if num_votes >= quorum:
                print("I'm the Leader now!")
                self.leader_addr = self.addr

            self.voting = False
            break

    async def look_for_leader(self):
        assert self.leader_addr is None

        results = await asyncio.gather(*[peer.look_for_leader() for peer in self.peers], return_exceptions=True)
        for index, result in enumerate(results):
            if result is not None:
                self.leader_addr = result
                print(f"Get Leader at {result}")
                break

    def on_look_for_leader(self) -> bytes:
        if self.leader_addr is not None:
            return b'{"ok":true,"value":"%s"}' % self.leader_addr.encode()
        else:
            return FAILURE

    def on_request_vote(self, data: bytes) -> bytes:
        if self.voting:
            print("I'm arranging an vote currently")
            return FAILURE

        rpc = fromdict(RequestVote, json.loads(data))

        if self.current_term > rpc.term:
            return FAILURE
        elif self.current_term == rpc.term and self.voted_for != rpc.candidate_addr:
            return FAILURE
        self.current_term = rpc.term

        if self.logs[-1].index > rpc.last_log_index or self.logs[-1].term > rpc.last_log_term:
            return FAILURE

        print("Voted for", rpc.candidate_addr)
        self.voted_for = rpc.candidate_addr

        return b'{"ok":true,"term":%d}' % rpc.term

    def run(self, message: bytes) -> bytes:
        if message == b"PING\r\n":
            return PONG
        elif message == b"LOOK_FOR_LEADER\r\n":
            return self.on_look_for_leader()
        elif message.startswith(b"REQUEST_VOTE "):
            return self.on_request_vote(message[13:])

        return f"Response from {self.addr} for message `{message.rstrip()}`".encode()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")

        try:
            while True:
                message = await reader.readline()
                print(f"Received {message} from {addr}")

                if not message or message.startswith(b"EXIT\r\n"):
                    print("Bye bye")
                    break

                data = self.run(message)

                print(f"Send: {data}")
                writer.write(data)
                writer.write(b"\r\n")
                await writer.drain()
        except Exception:
            print(f"!!!Exception occurred <peer: {addr}>!!!")
            traceback.print_exc()

        print("Close the connection")
        writer.close()


async def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <port> <peer1> [<peer2> [<peer3> [...]]]")
        return

    global LOOP
    LOOP = asyncio.get_event_loop()

    port, peers = sys.argv[1], sys.argv[2:]
    node = Node(f"localhost:{port}", peers)

    server = await asyncio.start_server(node.handle_connection, "localhost", port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}")

    async with server:
        await asyncio.gather(node.post_initialize(), node.request_votes(), server.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
