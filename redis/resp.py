from typing import Iterable, List, Optional, Tuple
from app.utils import print_action
from redis.command import Command
from redis.models import ReplicaConnection

def integer(n: int) -> bytes:
    return f':{n}\r\n'.encode()

def simple_string(string: str) -> bytes:
    return f'+{string}\r\n'.encode()

def simple_error(string: str) -> bytes:
    return f'-ERR {string}\r\n'.encode()

def error_unknown_command(array: List[str]) -> bytes:
    return simple_error(f'Unknown command {" ".join(array)}')

def bulk_string(string: Optional[str]) -> bytes:
    if string is None:
        return '$-1\r\n'.encode()
    else:
        string = str(string)
        return f'${len(string)}\r\n{string}\r\n'.encode()

def array(items) -> bytes:
    return (
        f'*{len(items)}\r\n'.encode()
        + b''.join(
            array(item) if type(item) is list else 
            bulk_string(item) 
            for item in items
        )
    )

def parse(buffer: bytes, db, connection: ReplicaConnection) -> Iterable[Command]:
    while buffer:
        # print_action(db.role, 'Parsing', buffer)
        if buffer.startswith(b'*'): # array
            array, buffer = _read_array(buffer)
            yield Command.parse(array, db, connection)
        elif buffer.startswith(b'+'): # simple string
            bytes, buffer = _read_token(buffer)
            print_action(db.role, 'Received', bytes)
        elif b'REDIS' in buffer:
            rdb, buffer = _read_rdb(buffer)
            print_action(db.role, 'Received', rdb)

def _read_array(buffer: bytes) -> Tuple[List[str], bytes]:
    array = []
    raw_length, buffer = _read_token(buffer[1:])
    length = int(raw_length.decode())
    for i in range(length):
        string, buffer = _read_bulk_string(buffer)
        array.append(string)
    return array, buffer

def _read_bulk_string(buffer) -> Tuple[str, bytes]:
    raw_length, buffer = _read_token(buffer)
    assert raw_length.startswith(b'$')
    length = int(raw_length[1:].decode())
    string = buffer[:length]
    return string.decode(), buffer[length+2:]

def _read_token(buffer: bytes) -> Tuple[bytes, bytes]:
    sep = buffer.index(b'\r\n')
    token = buffer[:sep]
    return token, buffer[sep+2:]

def _read_rdb(buffer: bytes) -> Tuple[bytes, bytes]:
    raw_length, buffer = _read_token(buffer)
    assert raw_length.startswith(b'$')
    length = int(raw_length[1:].decode())
    file = buffer[:length]
    return file, buffer[length:]
