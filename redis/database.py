import asyncio
import os
import time
from argparse import Namespace
from asyncio.streams import StreamReader, StreamWriter
from typing import Dict, List, Optional

import redis.resp as resp
from app.utils import print_action, random_string
from redis.models import ReplicaConnection
from redis.persistence import Persistence
from redis.stream import Stream, StreamTrigger

DEFAULT_PORT = 6379


class Database:
    @staticmethod
    def create(args: Namespace) -> 'Database':
        return DatabaseReplica(args) if args.replicaof else DatabaseMaster(args)

    def __init__(self, args: Namespace):
        self.store: Dict[str, tuple[str, Optional[float]]] = {}
        self.stream = Stream()
        self.config = vars(args)
        self.port = args.port or DEFAULT_PORT

        self.rdb_path = os.path.join(args.dir or '', args.dbfilename or '')
        if os.path.exists(self.rdb_path):
            self.persistence = Persistence(self)

    async def serve(self):
        server = await asyncio.start_server(
            self._handle_connection, 'localhost', self.port
        )
        print_action(self.role, f'Listening on port {self.port}.')
        async with server:
            await server.serve_forever()

    async def _handle_connection(
        self, reader: StreamReader, writer: StreamWriter, bufsize=1024
    ):
        while buffer := await reader.read(bufsize):
            await self._handle_incoming(buffer, ReplicaConnection(reader, writer))
        writer.close()

    async def _handle_incoming(self, buffer, connection: ReplicaConnection):
        for command in resp.parse(buffer, self, connection):
            await command.execute()

    def set(self, key: str, value: str, expiry: Optional[float] = None):
        self.store[key] = (value, expiry)

    def get(self, key: str) -> Optional[str]:
        value = None
        if item := self.store.get(key):
            value, expiry = item
            if expiry and time.time() * 1000 >= expiry:
                value = None
                del self.store[key]
        return value


class DatabaseMaster(Database):
    def __init__(self, args: Namespace):
        super().__init__(args)
        self.role = 'master'
        self.replicas: List[ReplicaConnection] = []
        self.master_replid = random_string(40)
        self.master_repl_offset = 0
        self.wait_triggers: List[StreamTrigger] = []
        self.stream_triggers: List[StreamTrigger] = []

    def register_replica(self, replica: ReplicaConnection):
        self.replicas.append(replica)
        print_action(self.role, f'Added replica from {replica.port}')

    def update_replica_offset(self, connection, offset):
        self.wait_triggers = [
            trigger for trigger in self.wait_triggers if not trigger.event.is_set()
        ]

        for replica in self.replicas:
            if connection.port == replica.port:
                replica.update_offset(offset)

        for trigger in self.wait_triggers:
            if (
                self.replication_count_acks_by_offset(trigger.master_offset)
                >= trigger.num_replicas
            ):
                trigger.event.set()

    async def propagate_command(self, command: bytes):
        self.master_repl_offset += len(command)
        for replica in self.replicas:
            print_action(self.role, f'Propagating to {replica.port}:', command)
            replica.writer.write(command)
            await replica.writer.drain()

    def check_stream_triggers(self, key: str, id: str):
        self.stream_triggers = [
            trigger for trigger in self.stream_triggers if not trigger.event.is_set()
        ]
        for trigger in self.stream_triggers:
            if any(
                key == condition[0] and id > condition[1]
                for condition in trigger.conditions
            ):
                trigger.event.set()

    def replication_count_acks_by_offset(self, master_offset):
        return sum(r.replication_offset >= master_offset for r in self.replicas)


class DatabaseReplica(Database):
    def __init__(self, args: Namespace):
        super().__init__(args)
        self.role = 'slave'
        master_host, master_port = args.replicaof
        self.replicaof = (master_host, int(master_port))
        self.master: ReplicaConnection
        self.slave_repl_offset = 0

    async def serve(self):
        await self._handshake()
        handle_master = super()._handle_connection(
            self.master.reader, self.master.writer
        )
        asyncio.create_task(handle_master)
        await super().serve()

    def inc_offset(self, n: int):
        self.slave_repl_offset += n

    async def _handshake(self):
        self.master = ReplicaConnection(*await asyncio.open_connection(*self.replicaof))
        await self._send_command('ping')
        await self._send_command('REPLCONF', 'listening-port', self.port)
        await self._send_command('REPLCONF', 'capa', 'psync2')
        await self._send_command('PSYNC', '?', '-1')

    async def _send_command(self, *strings: str) -> bytes:
        command = resp.array(strings)
        print_action(self.role, 'Sending', command)
        self.master.writer.write(command)
        await self.master.writer.drain()
        return await self._recv_response()

    async def _recv_response(self, bufsize=1024):
        buffer = await self.master.reader.read(bufsize)
        await super()._handle_incoming(buffer, self.master)
