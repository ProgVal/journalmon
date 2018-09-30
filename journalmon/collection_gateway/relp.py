"""Gateway listening for RELP messages."""

__all__ = ['RelpParseError', 'RawRelpFrame', 'RelpFrameStreamingParser',
        'parse_offers', 'serialize_offers']

from typing import List, Tuple, NamedTuple, Optional

MAX_DATALEN = 128000

class RelpParseError(Exception):
    pass

_RawRelpFrame = NamedTuple('_RawRelpFrame', [
    ('txid', 'int'),
    ('cmd', 'str'),
    ('data', 'bytes')
    ])

class RawRelpFrame(_RawRelpFrame):
    def serialize(self) -> bytes:
        if self.data:
            s = '{} {} {} '.format(self.txid, self.cmd, len(self.data))
            return s.encode('ascii') + self.data + b'\n'
        else:
            return '{} {} 0\n'.format(self.txid, self.cmd).encode('ascii')

class RelpFrameStreamingParser:
    def __init__(self) -> None:
        self._buffer = b''
        self._frames = [] # type: List[RawRelpFrame]
        self._reset_state()

    def _reset_state(self) -> None:
        self._current_txid = None # type: bytes
        self._current_cmd = None # type: bytes
        self._current_datalen = None # type: int

    def _on_frame_data(self) -> None:
        if len(self._buffer) >= self._current_datalen:
            frame_data = self._buffer[0:self._current_datalen]
            self._buffer = self._buffer[self._current_datalen:]
            frame = RawRelpFrame(
                    txid=int(self._current_txid),
                    cmd=self._current_cmd.decode('ascii'), # TODO: assert letters only
                    data=frame_data)
            self._frames.append(frame)
            self._reset_state()

    def _on_frame_header(self, parts: List[bytes]) -> None:
        if len(parts) == 3 and b'\n' in parts[2]:
            self._current_txid = parts[0]
            self._current_cmd = parts[1]
            subparts = parts[2].split(b'\n', 1)
            self._current_datalen = int(subparts[0])
            if self._current_datalen > MAX_DATALEN:
                raise RelpParseError('datalen is too large.')
            if len(subparts) == 1:
                self._buffer = subparts[1]
            else:
                self._buffer = b''
        elif len(parts) == 4:
            self._current_txid = parts[0]
            self._current_cmd = parts[1]
            self._current_datalen = int(parts[2])
            if self._current_datalen > MAX_DATALEN:
                raise RelpParseError('datalen is too large.')
            self._buffer = parts[3]

    def on_client_data(self, data: bytes) -> List[RawRelpFrame]:
        self._buffer += data
        while True:
            if self._current_txid is not None:
                assert self._current_cmd is not None
                assert self._current_datalen is not None
                self._on_frame_data()

            parts = self._buffer.split(b' ', 3)
            if len(parts) >= 3:
                self._on_frame_header(parts)
            else:
                break

        frames = self._frames
        self._frames = []
        return frames

def parse_offers(data: bytes) -> List[Tuple[bytes, Optional[bytes]]]:
    offers = [] # type: List[Tuple[bytes, bytes]]
    for line in data.split(b'\n'):
        if line == b'':
            pass # TODO: error?
        parts = line.split(b'=', 1)
        if len(parts) == 1:
            offers.append((parts[0], None))
        else:
            offers.append((parts[0], parts[1]))
    return offers

def serialize_offers(offers: List[Tuple[bytes, Optional[bytes]]]) -> bytes:
    return b'\n' + b'\n'.join(
            offer[0] if offer[1] is None else b'='.join(offer)
            for offer in offers
            )
