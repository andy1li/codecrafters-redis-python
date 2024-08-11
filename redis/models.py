import asyncio
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass


class ReplicaConnection:
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self.replication_offset = 0
        self.port = writer.get_extra_info('peername')[1]

    def __repr__(self):
        return f'Replica@{self.port}({self.replication_offset})'

    def update_offset(self, n: int):
        self.replication_offset = n


class WaitTrigger:
    def __init__(self, num_replicas, master_offset):
        self.event = asyncio.Event()
        self.num_replicas = num_replicas
        self.master_offset = master_offset
