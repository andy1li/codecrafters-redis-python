import struct
from typing import Any, Dict, Optional


class Persistence:
    def __init__(self, db: Any):
        self.db = db
        self.aux: Dict[str, str] = {}
        self.checksum: str
        self.db_size: int
        self.expiry_size: int
        self.version: int

        with open(self.db.rdb_path, 'rb') as file:
            self.file = file
            self._verify_magic()
            self._set_version()
            while True:
                try:
                    self._parse_next()
                except StopIteration:
                    break

    def _verify_magic(self):
        magic = self.file.read(5)
        assert magic == b'REDIS'

    def _set_version(self):
        version = self.file.read(4).decode()
        self.version = int(version)

    def _parse_next(self):
        opcode = self.file.read(1)
        # fmt: off
        match opcode:
            case b'\xFA': self._set_aux()
            case b'\xFB': self._set_resizedb()
            case b'\xFC': self._set_kv_pair_px()
            case b'\xFE': self._set_selectdb()
            case b'\xFF': self._set_checksum(); raise StopIteration
            case b'\x00': self._set_kv_pair()
        # fmt: on

    def _set_aux(self):
        key = self._read_string()
        value = self._read_string()
        self.aux[key] = value

    def _set_resizedb(self):
        self.db_size = self._read_length()
        self.expiry_size = self._read_length()

    def _set_selectdb(self):
        self.db_number = self._read_length()

    def _set_kv_pair(self, px=None):
        key = self._read_string()
        value = self._read_string()
        self.db.set(key, value, px)

    def _set_kv_pair_px(self):
        quadword = self.file.read(8)
        timestamp = struct.unpack('<Q', quadword)[0]
        self.file.read(1)
        self._set_kv_pair(timestamp)

    def _set_checksum(self):
        self.checksum = self.file.read(8)

    def _read_string(self) -> Optional[str | int]:
        length = self._read_length()
        if type(length) == int:
            return self.file.read(length).decode()
        if length == '8 bit integer':
            byte = self.file.read(1)
            return struct.unpack('B', byte)[0]
        return None

    def _read_length(self) -> Optional[int | str]:
        byte = self.file.read(1)
        length = struct.unpack('B', byte)[0]
        crumb = length >> 6
        # fmt: off
        match crumb:
            case 0b00: return length
            case 0b11: return '8 bit integer'
            case _   : return None
        # fmt: on
