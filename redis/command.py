import asyncio
import base64
import contextlib
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional

import redis.resp as resp
from app.utils import inc_id, print_action, to_pairs
from redis.models import ReplicaConnection, WaitTrigger
from redis.stream import StreamTrigger


@dataclass
class Command(ABC):
    array: List[str]
    db: Any
    connection: ReplicaConnection

    @staticmethod
    def parse(array: List[str], db, connection: ReplicaConnection) -> 'Command':
        name = array[0].upper()
        arguments = tuple(array[1:])
        print_action(
            db.role,
            f'Received {name}',
            ''
            if len(arguments) == 0
            else arguments[0]
            if len(arguments) == 1
            else ' '.join(arguments),
        )
        # fmt: off
        match name:
            case 'PING'     : return CommandPing(array, db, connection)
            case 'ECHO'     : return CommandEcho(array, db, connection)
            case 'SET'      : return CommandSet(array, db, connection)
            case 'GET'      : return CommandGet(array, db, connection)
            case 'CONFIG'   : return CommandConfig(array, db, connection)
            case 'KEYS'     : return CommandKeys(array, db, connection)
            case 'INFO'     : return CommandInfo(array, db, connection)
            case 'REPLCONF' : return CommandReplConf(array, db, connection)
            case 'PSYNC'    : return CommandPSync(array, db, connection)
            case 'WAIT'     : return CommandWait(array, db, connection)
            case 'TYPE'     : return CommandType(array, db, connection)
            case 'XADD'     : return CommandXAdd(array, db, connection)
            case 'XRANGE'   : return CommandXRange(array, db, connection)
            case 'XREAD'    : return CommandXRread(array, db, connection)
            case _          : return CommandUnknown(array, db, connection)
        # fmt: on

    async def execute(self):
        response = await self._respond()

        if self.db.role == 'slave':
            offset = len(self._pack())
            self.db.inc_offset(offset)

        if response:
            print_action(self.db.role, 'Sending', response)
            self.connection.writer.write(response)
            await self.connection.writer.drain()

    def _pack(self) -> bytes:
        return resp.array(self.array)

    @abstractmethod
    async def _respond(self) -> bytes | None: ...


class CommandUnknown(Command):
    async def _respond(self) -> bytes:
        return resp.error_unknown_command(self.array)


class CommandPing(Command):
    async def _respond(self) -> Optional[bytes]:
        if self.db.role == 'master':
            return resp.simple_string('PONG')
        return None


class CommandEcho(Command):
    async def _respond(self) -> bytes:
        return resp.bulk_string(self.array[1])


class CommandSet(Command):
    async def _respond(self) -> Optional[bytes]:
        key, value = self.array[1:3]
        px = (
            int(self.array[4])
            if (len(self.array) == 5 and self.array[3].upper() == 'PX')
            else None
        )
        expiry = px and (time.time() * 1000 + px)

        self.db.set(key, value, expiry)
        if self.db.role == 'master':
            propagate_command = self.db.propagate_command(self._pack())
            asyncio.create_task(propagate_command)
            return resp.simple_string('OK')
        return None


class CommandGet(Command):
    async def _respond(self) -> bytes:
        value = self.db.get(self.array[1])
        return resp.bulk_string(value)


class CommandConfig(Command):
    async def _respond(self) -> bytes:
        subcommand = self.array[1].upper()
        match subcommand:
            case 'GET':
                key = self.array[2]
                value = self.db.config.get(key)
                return resp.array([key, value])
            case _:
                return resp.error_unknown_command(self.array)


class CommandKeys(Command):
    async def _respond(self) -> bytes:
        keys = self.db.store.keys()
        return resp.array(keys)


class CommandInfo(Command):
    async def _respond(self) -> bytes:
        info = '# Replication\n'
        info += f'role:{self.db.role}\n'
        if self.db.role == 'master':
            info += f'master_replid:{self.db.master_replid}\n'
            info += f'master_repl_offset:{self.db.master_repl_offset}\n'
        return resp.bulk_string(info)


class CommandReplConf(Command):
    async def _respond(self) -> Optional[bytes]:
        subcommand = self.array[1].upper()
        match subcommand:
            case 'LISTENING-PORT':
                return resp.simple_string('OK')
            case 'CAPA':
                return resp.simple_string('OK')
            case 'GETACK':
                if self.db.role == 'slave':
                    return resp.array(['REPLCONF', 'ACK', self.db.slave_repl_offset])
            case 'ACK':
                if self.db.role == 'master':
                    offset = int(self.array[2])
                    self.db.update_replica_offset(self.connection, offset)
            case _:
                return resp.error_unknown_command(self.array)
        return None


class CommandPSync(Command):
    async def _respond(self) -> bytes:
        if self.db.role == 'master':
            self.db.register_replica(self.connection)

        RDB_BASE64 = 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='
        RDB_BYTES = base64.b64decode(RDB_BASE64)
        return (
            resp.simple_string(
                f'FULLRESYNC {self.db.master_replid} {self.db.master_repl_offset}'
            )
            + f'${len(RDB_BYTES)}\r\n'.encode()
            + RDB_BYTES
        )


class CommandWait(Command):
    async def _respond(self) -> Optional[bytes]:
        if self.db.role == 'master':
            num_replicas, timeout = map(int, self.array[1:])
            master_offset = self.db.master_repl_offset

            ack_replicas = self.db.replication_count_acks_by_offset(master_offset)
            if ack_replicas >= num_replicas:
                return resp.integer(ack_replicas)

            trigger = WaitTrigger(num_replicas, master_offset)
            self.db.wait_triggers.append(trigger)
            get_ack = resp.array('REPLCONF GETACK *'.split())

            if master_offset > 0:
                await self.db.propagate_command(get_ack)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(trigger.event.wait(), timeout / 1000)

            ack_replicas = self.db.replication_count_acks_by_offset(
                trigger.master_offset
            )
            return resp.integer(ack_replicas)

        return None


class CommandType(Command):
    async def _respond(self) -> bytes:
        key = self.array[1]
        if self.db.get(key) is not None:
            return resp.simple_string('string')
        if self.db.stream.read(key) is not None:
            return resp.simple_string('stream')
        else:
            return resp.simple_string('none')


class CommandXAdd(Command):
    async def _respond(self) -> bytes:
        key, id = self.array[1:3]
        entry = [id, self.array[3:]]
        self.db.check_stream_triggers(key, id)
        return self.db.stream.add(key, entry)


class CommandXRange(Command):
    async def _respond(self) -> bytes:
        key, start, end = self.array[1:]
        entries = self.db.stream.range(key, start, end)
        return resp.array(entries)


class CommandXRread(Command):
    def _read(self, request) -> bytes:
        result = [
            [key, self.db.stream.range(key, inc_id(start), '+')]
            for key, start in to_pairs(request)
        ]
        is_empty = all(not len(stream[1]) for stream in result)
        return resp.bulk_string(None) if is_empty else resp.array(result)

    def _replace_dollars(self):
        for i, (key, id) in enumerate(to_pairs(self.array[4:])):
            if id == '$':
                entries = self.db.stream.read(key)
                self.array[4 + 2 * i + 1] = (
                    '0-0' if not len(entries) else entries[-1][0]
                )

    async def _respond(self) -> Optional[bytes]:
        subcommand = self.array[1].upper()
        match subcommand:
            case 'STREAMS':
                return self._read(self.array[2:])
            case 'BLOCK':
                self._replace_dollars()
                trigger = StreamTrigger(self.array[4:])
                self.db.stream_triggers.append(trigger)
                timeout = (int(self.array[2]) / 1000) or sys.maxsize
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(trigger.event.wait(), timeout)
                return self._read(self.array[4:])
        return None
