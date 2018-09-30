"""Gateway listening for RELP messages."""

__all__ = ['RelpError', 'RelpParseError', 'RelpVersionError',
        'RawRelpFrame', 'RelpFrameStreamingParser',
        'parse_offers', 'serialize_offers', 'RelpSession']

from enum import Enum
from typing import List, Tuple, NamedTuple, Optional

from syslog_rfc5424_parser import SyslogMessage

MAX_DATALEN = 128000

class RelpError(Exception):
    pass

class RelpParseError(RelpError):
    pass

class RelpVersionError(RelpError):
    pass

_RawRelpFrame = NamedTuple('_RawRelpFrame', [
    ('txid', 'int'),
    ('cmd', 'str'),
    ('data', 'bytes')
    ])

class RawRelpFrame(_RawRelpFrame):
    def serialize(self) -> bytes:
        if self.data:
            s = '{} {} {} '.format(self.txid, self.cmd, len(self.data)+1-1)
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
        if len(self._buffer) >= self._current_datalen+1:
            frame_data = self._buffer[0:self._current_datalen]
            assert self._buffer[self._current_datalen] == ord('\n'), self._buffer
            self._buffer = self._buffer[self._current_datalen+1:]
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
                self._buffer = b'\n' + subparts[1]
            else:
                self._buffer = b'\n'
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
            continue
        parts = line.split(b'=', 1)
        if len(parts) == 1:
            offers.append((parts[0], None))
        else:
            offers.append((parts[0], parts[1]))
    return offers

def serialize_offers(client_version: int,
        offers: List[Tuple[bytes, Optional[bytes]]]) -> bytes:
    if client_version == 0:
        prefix = b'' # XXX: Is this ok? That's what librelp0 does in Debian 9
    elif client_version == 1:
        prefix = b'\n' # XXX: that's what the spec says
    return prefix + b'\n'.join(
            offer[0] if offer[1] is None else b'='.join(offer)
            for offer in offers
            )

class SessionState(Enum):
    UNINITIALIZED = 0
    OPEN = 1
    CLOSED = 2

class RelpSession:
    def __init__(self):
        self._state = SessionState.UNINITIALIZED
        self._parser = RelpFrameStreamingParser()
        self._client_version = None
        self.out_buffer = [] # type: List[bytes]
        self.messages = [] # type: List[SyslogMessage]

    @property
    def closed(self) -> bool:
        return self._state == SessionState.CLOSED

    def on_client_data(self, data: bytes) -> None:
        """Called when some `data` is received from the client."""
        for frame in self._parser.on_client_data(data):
            if self._state == SessionState.UNINITIALIZED:
                if frame.cmd == 'open':
                    self._on_open_session(frame)
                else:
                    raise RelpError('Unknown or unexpected command: {}'
                            .format(frame.cmd))
            elif self._state == SessionState.OPEN:
                if frame.cmd == 'syslog':
                    msg = frame.data # TODO: parse
                    self.messages.append((frame.txid, msg))
                elif frame.cmd == 'close':
                    self._state = SessionState.CLOSED
                    frame = RawRelpFrame(txid=frame.txid, cmd='rsp', data=None)
                    self.out_buffer.append(frame.serialize())
                else:
                    raise RelpError('Unknown or unexpected command: {}'
                            .format(frame.cmd))
            elif self._state == SessionState.CLOSED:
                raise RelpError('Got data after session was closed.')
            else:
                assert False

    def _on_open_session(self, frame: RawRelpFrame):
        client_offers = dict(parse_offers(frame.data))
        client_version = int(client_offers[b'relp_version'].decode('ascii'))
        if client_version not in (0, 1):
            raise RelpVersionError('RELP version {} is not supported'
                    .format(client_version))
        self._state = SessionState.OPEN
        self._client_version = client_version
        offers = [
                (b'relp_version', str(client_version).encode('ascii')),
                (b'relp_software', b'journalmon'),
                (b'commands', b'syslog'),
                ]
        data = serialize_offers(client_version, offers)
        response = RawRelpFrame(txid=frame.txid, cmd='rsp', data=data)
        self.out_buffer.append(response.serialize())

    def ack_msg(self, msg_id: int) -> None:
        """Acknowledge a message has been hanlded."""
        frame = RawRelpFrame(txid=msg_id, cmd='rsp', data=b'200 OK')
        self.out_buffer.append(frame.serialize())

