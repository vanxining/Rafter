import asyncio
import json
import logging
import random
import sys

from dataclasses import dataclass
from datetime import datetime

from dataclass_wizard import asdict, fromdict

import util


LOGGER = logging.getLogger("rafter")

EPOCH = datetime.fromtimestamp(0)
LOOP: asyncio.AbstractEventLoop


@dataclass
class RequestVote:
    term: int
    candidate_addr: str
    last_log_index: int
    last_log_term: int


@dataclass
class Log:
    index: int
    term: int


@dataclass
class AppendEntries:
    term: int

    leader_addr: str
    leader_commit_index: int

    entries: list[Log]

    # For validation
    prev_log_index: int
    prev_log_term: int


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
            LOGGER.exception(f"Failed to connect to peer at {self.addr}")
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
            LOGGER.exception(f"Failed to ping peer at {self.addr}")

        return False

    async def look_for_leader(self):
        if not self.is_connected():
            return None

        try:
            response = await self.send(b"LOOK_FOR_LEADER")
            if response and response.rstrip() != FAILURE:
                obj = json.loads(response)
                if obj.get("ok") is True:
                    return obj.get("value")
        except Exception:
            LOGGER.exception(f"Failed to query peer at {self.addr} for Leader")

        return None

    async def request_vote(self, rpc: RequestVote):
        if not self.is_connected():
            return False

        try:
            response = await self.send(f"REQUEST_VOTE {util.dump_as_json(asdict(rpc))}")
            if response and response.rstrip() != FAILURE:
                obj = json.loads(response)
                if obj.get("ok") is True and obj.get("term") == rpc.term:
                    return True
        except Exception:
            LOGGER.exception(f"Failed to send RequestVote RPC to peer at {self.addr}")

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
        self.next_indices = []
        self.match_indices = []

        # Cluster status
        self.peers = [Peer(addr) for addr in peers]

    async def post_initialize(self):
        results = await asyncio.gather(*[Node.initialize_peer(peer) for peer in self.peers], return_exceptions=True)
        for index, result in enumerate(results):
            connected, leader_addr = result
            if connected is True:
                LOGGER.info(f"Connected to peer at {self.peers[index].addr}")
                if leader_addr is not None:
                    if self.leader_addr is not None and self.leader_addr != leader_addr:
                        if self.leader_addr != leader_addr:
                            raise RuntimeError("More than one Leader in the cluster!")
                    else:
                        LOGGER.info(f"Get Leader at {leader_addr}")
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
                LOGGER.info(f"Can't ping peer at {self.peers[index].addr}")

    def on_look_for_leader(self) -> bytes:
        if self.leader_addr is not None:
            return b'{"ok":true,"value":"%s"}' % self.leader_addr.encode()
        else:
            return FAILURE

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
            LOGGER.info(f"Start voting for new term {self.current_term}")

            rpc = RequestVote(self.current_term, self.addr, self.logs[-1].index, self.logs[-1].term)

            num_votes = 1
            results = await asyncio.gather(*[peer.request_vote(rpc) for peer in self.peers], return_exceptions=True)
            for index, result in enumerate(results):
                LOGGER.info(f"Peer at {self.peers[index].addr} {'voted for me' if result else 'NOT vote for me'}")
                if result:
                    num_votes += 1

            LOGGER.info(f"Cluster size: {len(self.peers) + 1}, quorum: {quorum}, votes: {num_votes}")
            if num_votes >= quorum:
                LOGGER.info("I'm the Leader now!")

                self.leader_addr = self.addr
                self.next_indices = [len(self.logs)] * len(self.peers)
                self.match_indices = [len(self.logs) - 1] * len(self.peers)

                await self.append_entries()

            self.voting = False

    def on_request_vote(self, data: bytes) -> bytes:
        if self.voting:
            LOGGER.info("I'm arranging an vote currently")
            return FAILURE

        rpc = fromdict(RequestVote, json.loads(data))

        if self.current_term > rpc.term:
            return FAILURE
        elif self.current_term == rpc.term and self.voted_for != rpc.candidate_addr:
            return FAILURE
        self.current_term = rpc.term

        if self.logs[-1].index > rpc.last_log_index or self.logs[-1].term > rpc.last_log_term:
            return FAILURE

        LOGGER.info(f"Voted for {rpc.candidate_addr}")
        self.voted_for = rpc.candidate_addr

        return b'{"ok":true,"term":%d}' % rpc.term

    async def append_entries(self):
        await asyncio.gather(
            *[self.append_entries_for_peer(index) for index in range(len(self.peers))], return_exceptions=True)

    async def append_entries_for_peer(self, peer_index: int):
        next_index = self.next_indices[peer_index]
        new_next_index = len(self.logs)
        previous_entry = self.logs[self.match_indices[peer_index]]

        rpc = AppendEntries(self.current_term, self.addr, self.commit_index,
                            self.logs[next_index:], previous_entry.index, previous_entry.term)

        response = await self.peers[peer_index].send("APPEND_ENTRIES " + util.dump_as_json(asdict(rpc)))
        if response.rstrip() != FAILURE:
            obj = json.loads(response)
            if obj.get("ok") is True:
                self.next_indices[peer_index] = new_next_index
                self.match_indices[peer_index] = new_next_index - 1

    def on_append_entries(self, data: bytes) -> bytes:
        rpc = fromdict(AppendEntries, json.loads(data))

        if self.current_term > rpc.term:
            return FAILURE
        if self.current_term < rpc.term:
            self.current_term = rpc.term
        if self.leader_addr != rpc.leader_addr:  # Switch to the new leader
            self.leader_addr = rpc.leader_addr

        if self.logs[-1].index != rpc.prev_log_index:
            return FAILURE

        if self.logs[-1].term != rpc.term and len(self.logs) > 1:
            self.logs = self.logs[:-1]
            return FAILURE

        if len(rpc.entries) > 0:
            self.logs += rpc.entries

        return b'{"ok":true}'

    def run(self, message: bytes) -> bytes:
        if message == b"PING\r\n":
            return PONG
        elif message.startswith(b"APPEND_ENTRIES "):
            return self.on_append_entries(message[15:])
        elif message == b"LOOK_FOR_LEADER\r\n":
            return self.on_look_for_leader()
        elif message.startswith(b"REQUEST_VOTE "):
            return self.on_request_vote(message[13:])

        return f"General response from {self.addr} for unknown message `{message.rstrip()}`".encode()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")

        try:
            while True:
                message = await reader.readline()
                LOGGER.info(f"Received {message} from {addr}")

                if not message or message.startswith(b"EXIT\r\n"):
                    LOGGER.info("Bye bye")
                    break

                data = self.run(message)

                LOGGER.debug(f"Send: {data}")
                writer.write(data)
                writer.write(b"\r\n")
                await writer.drain()
        except Exception:
            LOGGER.exception(f"!!!Exception occurred <peer: {addr}>!!!")

        LOGGER.debug("Close the connection")
        writer.close()


async def main():
    if len(sys.argv) < 3:
        logging.error(f"Usage: {sys.argv[0]} <port> <peer1> [<peer2> [<peer3> [...]]]")
        return

    logging.getLogger().setLevel(logging.WARNING)
    LOGGER.propagate = False
    LOGGER.handlers = util.create_logging_handlers()
    LOGGER.setLevel(logging.DEBUG)

    global LOOP
    LOOP = asyncio.get_event_loop()

    port, peers = sys.argv[1], sys.argv[2:]
    node = Node(f"localhost:{port}", peers)

    server = await asyncio.start_server(node.handle_connection, "localhost", port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    LOGGER.info(f"Serving on {addrs}")

    async with server:
        await asyncio.gather(node.post_initialize(), node.request_votes(), server.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
