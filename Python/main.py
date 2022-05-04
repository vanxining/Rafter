#!/usr/bin/env python3

import asyncio
import json
import logging
import random
import sys

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from aiorun import run
from dataclass_wizard import asdict, fromdict

import util


LOGGER = logging.getLogger("rafter")


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
    command: str


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
OK = b'{"ok":true}'
FAILURE = b'{"ok":false}'
FAILURE_WITH_REASON = b'{"ok":false,"reason":"%b"}'
NOOP = ""

EPOCH = datetime.fromtimestamp(0)
CONNECT_TIMEOUT = timedelta(milliseconds=50)
LEADER_HEARDBEAT_TIMEOUT = timedelta(milliseconds=800)
FOLLOWER_HEARDBEAT_TIMEOUT = timedelta(seconds=3)
VOTE_TIMEOUT = LEADER_HEARDBEAT_TIMEOUT * 1.5


def is_peer_change_command(command: str):
    return command.startswith("ADD_PEER ") or command.startswith("REMOVE_PEER ")


class Peer(object):
    def __init__(self, addr: str):
        self.addr = addr
        self.host, self.port = addr.split(":")

        self.reader = None
        self.writer = None

        self.is_first_time_initialized = False
        self.last_active_time = EPOCH

    async def initialize(self):
        try:
            LOGGER.info(f"Connecting peer at {self.addr}")

            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=CONNECT_TIMEOUT.total_seconds())
            self.last_active_time = datetime.now()

            LOGGER.info(f"Connected to peer at {self.addr}")
            return True
        except Exception:
            LOGGER.warning(f"Failed to connect to peer at {self.addr}")
            return False
        finally:
            if not self.is_first_time_initialized:
                self.is_first_time_initialized = True

    async def finalize(self):
        try:
            if self.writer is not None:
                self.writer.close()
                await self.writer.wait_closed()

            self.reader = self.writer = None
        except Exception:
            LOGGER.exception(f"Failed to close write stream for peer at {self.addr}")

    def is_connected(self):
        return (self.reader is not None and not self.reader.at_eof() and
                self.writer is not None and not self.writer.is_closing())

    async def ensure_connected(self):
        if self.is_connected():
            return True
        elif not self.is_first_time_initialized:
            raise RuntimeError(f"Please wait for peer at {self.addr} to be initialized for the first time!")
        else:
            LOGGER.info(f"Re-connecting peer at {self.addr}")
            await self.finalize()
            return await self.initialize()

    async def send(self, message: [bytes, str]) -> bytes:
        if isinstance(message, str):
            message = message.encode()

        self.writer.write(message)
        self.writer.write(b"\r\n")
        await self.writer.drain()

        return await self.reader.readline()

    async def ping(self):
        try:
            if await self.ensure_connected():
                response = await self.send(b"PING")
                if response.rstrip() == PONG:
                    self.last_active_time = datetime.now()
                    return True
        except Exception:
            LOGGER.exception(f"Failed to ping peer at {self.addr}")

        return False

    async def look_for_leader(self):
        if not self.is_connected():
            return None

        try:
            response = await self.send(b'LOOK_FOR_LEADER')
            if response and response.rstrip() != FAILURE:
                obj = json.loads(response)
                if obj.get("ok") is True:
                    return obj.get("leader_addr")
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

    async def append_entries(self, rpc: AppendEntries) -> [bool, None]:
        assert self.is_connected()

        try:
            response = await self.send(f"APPEND_ENTRIES {util.dump_as_json(asdict(rpc))}")
            if response:
                if response.rstrip() != FAILURE:
                    obj = json.loads(response)
                    if obj.get("ok") is True:
                        return True
                else:
                    return False
            LOGGER.exception(f"Unexpected AppendEntries RPC response from peer at {self.addr} -- {response}")
        except Exception:
            LOGGER.exception(f"Failed to send AppendEntries RPC to peer at {self.addr}")

        return None


class Node(object):
    def __init__(self, addr: str, peers: list[str]):
        self.addr = addr

        self.logs = [Log(0, 0, NOOP)]
        self.commit_index = 0
        self.last_applied = 0

        self.current_term = 0

        self.voted_for = self.addr
        self.vote_timeout = VOTE_TIMEOUT * random.uniform(1.0, 1.2)
        self.is_candidate = False

        self.leader_addr = None
        self.leader_last_active_time = EPOCH

        # Only for Leader
        self.next_indices = []
        self.match_indices = []

        # Cluster status
        self.peers = [Peer(addr) for addr in peers]

    async def post_initialize(self):
        results = await asyncio.gather(*(Node.initialize_peer(peer) for peer in self.peers))
        for index, result in enumerate(results):
            connected, leader_addr = result
            if connected is True:
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

    async def finalize(self):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            LOGGER.info("Finalizing all peer connections...")
            await asyncio.gather(*(peer.finalize() for peer in self.peers))

    @property
    def is_leader(self):
        return self.addr == self.leader_addr

    def get_quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def update_commit_index(self):
        peer_quorum = self.get_quorum() - 1
        count = defaultdict(int)

        for index in self.match_indices:
            count[index] += 1
            if index > 0 and count[index] >= peer_quorum:
                assert self.commit_index <= index
                if self.commit_index < index:
                    if self.logs[index].term == self.current_term:
                        self.apply_peer_changes(self.commit_index + 1, index + 1)
                        self.commit_index = index
                        LOGGER.info(f"Commit index updated to {index}")
                    else:
                        LOGGER.info(f"Skipped to commmit Log {index} because it was not produced in my term")
                break

    def on_look_for_leader(self) -> bytes:
        if self.leader_addr is not None:
            return b'{"ok":true,"leader_addr":"%s"}' % self.leader_addr.encode()
        else:
            return FAILURE

    async def request_votes(self):
        while True:
            await asyncio.sleep(self.vote_timeout.total_seconds())

            if self.leader_addr is not None:
                if self.is_leader:
                    continue
                delta = datetime.now() - self.leader_last_active_time
                if delta < self.vote_timeout:
                    continue
                else:
                    self.leader_addr = None
                    LOGGER.info("Leader is not responding. A new election will be started")

            peers = self.peers.copy()
            quorum = self.get_quorum()

            num_connected_nodes = 1
            for peer in peers:
                if peer.is_connected():
                    num_connected_nodes += 1
            if num_connected_nodes < quorum:
                LOGGER.info(f"No enough active nodes to start a election. "
                            f"Total: {len(peers) + 1}, quorum: {quorum}, active: {num_connected_nodes}")
                continue

            self.current_term += 1
            self.voted_for = self.addr
            self.is_candidate = True
            LOGGER.info(f"Start election for Term {self.current_term}")

            rpc = RequestVote(self.current_term, self.addr, self.logs[-1].index, self.logs[-1].term)

            num_votes = 1
            results = await asyncio.gather(*(peer.request_vote(rpc) for peer in peers))
            for index, result in enumerate(results):
                LOGGER.info(f"Peer at {peers[index].addr} {'voted for me' if result else 'NOT vote for me'}")
                if result:
                    num_votes += 1

            LOGGER.info(f"Cluster size: {len(peers) + 1}, quorum: {quorum}, votes: {num_votes}")
            if rpc.term != self.current_term:
                assert rpc.term < self.current_term
                LOGGER.info(f"Term {self.current_term} started by peer. Election aborted")
            elif num_votes >= quorum:
                LOGGER.info(f"I'm the Leader in Term {self.current_term} now!")

                self.leader_addr = self.addr
                self.next_indices = [len(self.logs)] * len(peers)
                self.match_indices = [0] * len(peers)

                await self.append_entries()

            self.is_candidate = False

    def on_request_vote(self, data: bytes) -> bytes:
        rpc = fromdict(RequestVote, json.loads(data))
        assert rpc.candidate_addr != self.addr

        if self.logs[-1].index > rpc.last_log_index or self.logs[-1].term > rpc.last_log_term:
            return FAILURE

        if self.current_term > rpc.term:
            return FAILURE
        elif self.current_term == rpc.term and self.voted_for != rpc.candidate_addr:
            return FAILURE
        self.current_term = rpc.term

        LOGGER.info(f"Voted for {rpc.candidate_addr}")
        self.voted_for = rpc.candidate_addr

        return b'{"ok":true,"term":%d}' % rpc.term

    async def append_entries(self):
        await asyncio.gather(*(self.append_entries_for_peer(index) for index in range(len(self.peers))))
        self.update_commit_index()  # Caution: cluster size change

    async def append_entries_for_peer(self, peer_index: int):
        peer = self.peers[peer_index]
        if not peer.is_first_time_initialized or not await peer.ensure_connected():
            return

        next_index = self.next_indices[peer_index]
        new_next_index = min(next_index + 5, len(self.logs))
        previous_entry = self.logs[next_index - 1]

        rpc = AppendEntries(self.current_term, self.addr, self.commit_index,
                            self.logs[next_index:new_next_index].copy(),
                            previous_entry.index, previous_entry.term)

        result = await peer.append_entries(rpc)
        if result is True:
            self.next_indices[peer_index] = new_next_index
            self.match_indices[peer_index] = new_next_index - 1
        elif result is False:
            if next_index > 1:
                self.next_indices[peer_index] = next_index - 1
                self.match_indices[peer_index] = 0
            else:
                raise RuntimeError("Unexpected condition: dummy logs not align")

    def on_append_entries(self, data: bytes) -> bytes:
        rpc = fromdict(AppendEntries, json.loads(data))

        if self.current_term > rpc.term:
            return FAILURE
        if self.current_term < rpc.term:
            self.current_term = rpc.term
        if self.leader_addr != rpc.leader_addr:  # Switch to the new leader
            self.leader_addr = rpc.leader_addr
        self.leader_last_active_time = datetime.now()

        if self.logs[-1].term != rpc.prev_log_term:
            if len(self.logs) > 1:  # Not the dummy log case
                self.logs = self.logs[:-1]
            return FAILURE

        if self.logs[-1].index != rpc.prev_log_index:
            if self.logs[-1].index > rpc.prev_log_index:
                raise RuntimeError("Different logs of the same slot in the same term")
            return FAILURE

        if len(rpc.entries) > 0:
            self.logs += rpc.entries
        last_commit_index = self.commit_index
        self.commit_index = min(len(self.logs) - 1, rpc.leader_commit_index)

        self.apply_peer_changes(last_commit_index + 1, self.commit_index + 1)

        return OK

    def apply_peer_changes(self, begin, end):
        for i in range(begin, end):
            if is_peer_change_command(self.logs[i].command):
                self.on_peer_change(self.logs[i].command)

    def leader_on_peer_change(self, command: str) -> bytes:
        if not self.is_leader:
            LOGGER.error(f"Not the Leader but received a {command[:command.index(' ')]} message")
            return FAILURE

        self.logs.append(Log(len(self.logs), self.current_term, command))

        return OK

    @staticmethod
    async def leader_notify_removed_peer(peer: Peer):
        try:
            if peer.is_connected():
                await peer.send("LEAVE_CLUSTER")
        except Exception:
            LOGGER.exception(f"Failed to notify peer at {peer.addr} to leave the cluster")
        finally:
            await peer.finalize()

    def on_peer_change(self, command: str):
        addr = command[(command.index(" ") + 1):]

        found = False
        index = -1
        for index, peer in enumerate(self.peers):
            if peer.addr == addr:
                found = True
                break

        if command.startswith("ADD_PEER "):
            if found or self.addr == addr:
                return

            peer = Peer(addr)
            peer.is_first_time_initialized = True  # Hack
            self.peers.append(peer)

            if self.is_leader:
                self.next_indices.append(len(self.logs))
                self.match_indices.append(0)
        elif command.startswith("REMOVE_PEER "):
            loop = asyncio.get_event_loop()

            if self.addr == addr:
                loop.stop()
                LOGGER.info("Leave the cluster by myself")
                return

            if not found:
                return

            peer = self.peers.pop(index)
            if self.is_leader:
                loop.call_soon(lambda: asyncio.ensure_future(Node.leader_notify_removed_peer(peer)))
                del self.next_indices[index]
                del self.match_indices[index]
            else:
                loop.call_soon(lambda: asyncio.ensure_future(peer.finalize()))

    def on_mod(self) -> bytes:
        if not self.is_leader:
            LOGGER.error("Not the Leader but received a MOD message")
            return FAILURE

        self.logs.append(Log(len(self.logs), self.current_term, NOOP))

        return OK

    async def leader_send_heartbeat(self):
        while True:
            await asyncio.sleep(LEADER_HEARDBEAT_TIMEOUT.total_seconds())

            if self.is_leader and not self.is_candidate:
                await self.append_entries()

    async def follower_send_heartbeat(self):
        while True:
            await asyncio.sleep(FOLLOWER_HEARDBEAT_TIMEOUT.total_seconds())

            if not self.is_leader and not self.is_candidate:
                await asyncio.gather(*(peer.ping() for peer in self.peers
                                       if peer.is_first_time_initialized and peer.addr != self.leader_addr))

    def run(self, message: bytes) -> bytes:
        if message == b"PING\r\n":
            return PONG
        elif message.startswith(b"APPEND_ENTRIES "):
            return self.on_append_entries(message[15:])
        elif message.startswith(b"MOD\r\n"):
            return self.on_mod()
        elif message.startswith(b"REQUEST_VOTE "):
            return self.on_request_vote(message[13:])
        elif message == b"LOOK_FOR_LEADER\r\n":
            return self.on_look_for_leader()
        elif message == b"LEAVE_CLUSTER\r\n":
            loop = asyncio.get_event_loop()
            loop.call_soon(loop.stop)
            return OK
        else:
            decoded = message.decode().rstrip()
            if is_peer_change_command(decoded):
                return self.leader_on_peer_change(decoded)

        return f"General response for unknown message {message.rstrip()}".encode()

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")

        try:
            while True:
                message = await reader.readline()
                LOGGER.debug(f"Received {message} from {addr}")

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
        finally:
            LOGGER.debug("Close the connection")
            writer.close()
            await writer.wait_closed()


async def main():
    if (len(sys.argv) < 4) or (int(sys.argv[1]) < 0) or (int(sys.argv[1]) >= len(sys.argv) - 2):
        sys.stderr.write(f"Usage: {sys.argv[0]} <my_addr_index> <addr1> <addr2> [<addr3> [<addr4> [...]]]\n")
        asyncio.get_event_loop().stop()
        return

    logging.getLogger().setLevel(logging.WARNING)
    LOGGER.propagate = False
    LOGGER.handlers = util.create_logging_handlers()
    LOGGER.setLevel(logging.DEBUG)

    my_index = 2 + int(sys.argv[1])
    my_addr = sys.argv[my_index]
    host, port = my_addr.split(":")
    peers = sys.argv[2:my_index] + sys.argv[(my_index + 1):]

    node = Node(my_addr, peers)
    server = await asyncio.start_server(node.handle_connection, host, port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    LOGGER.info(f"Serving on {addrs}")

    async with server:
        await asyncio.gather(node.post_initialize(),
                             node.finalize(),
                             node.request_votes(),
                             node.leader_send_heartbeat(),
                             node.follower_send_heartbeat(),
                             server.serve_forever())


if __name__ == "__main__":
    run(main())
