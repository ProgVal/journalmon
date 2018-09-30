import sys
import socket
from typing import List, Tuple
from socketserver import TCPServer, StreamRequestHandler, ThreadingMixIn

from ..storage_backends.sqlite3_storage import Sqlite3Storage
from .relp import RelpSession

class RelpRequestHandler(StreamRequestHandler):
    def handle(self):
        self.request.settimeout(0.1)
        s = RelpSession()
        while not s.closed:
            try:
                s.on_client_data(self.request.recv(2048))
            except socket.timeout:
                pass
            print(repr(s.messages))
            s.messages = []
            for b in s.out_buffer:
                self.request.send(b)
            s.out_buffer = []

class RelpServer(TCPServer, ThreadingMixIn):
    pass

def main(*, db_name: str, addr: Tuple[str, int]):
    db = Sqlite3Storage(db_name)
    RelpServer(addr, RelpRequestHandler).serve_forever()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        db_name = sys.argv[1]
        port = ('0.0.0.0', 2514)
    elif len(sys.argv) == 3:
        db_name = sys.argv[1]
        (ip, port) = sys.argv[2].rsplit(':', 1) # type: ignore
        addr = (ip, int(port)) # type: ignore
    else:
        print('Syntax: {} <db_name.sqlite3> [<address:port>]')
        exit(1)
    main(db_name=db_name, addr=addr)
