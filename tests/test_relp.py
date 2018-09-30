import random
from unittest import TestCase

from journalmon.collection_gateway.relp import *

LIBRELP_OPEN_FRAME = b'''1 open 86 relp_version=0
relp_software=librelp,1.2.12,http://librelp.adiscon.com
commands=syslog
'''
LIBRELP_OPEN_FRAME_PARSED = RawRelpFrame(
        txid=1, cmd='open',
        data=b'relp_version=0\nrelp_software=librelp,1.2.12,http://librelp.adiscon.com\ncommands=syslog'
        )


class TestRelpParser(TestCase):
    def test_frame(self):
        p = RelpFrameStreamingParser()

        self.assertEqual(p.on_client_data(b''), [])
        self.assertEqual(p.on_client_data(LIBRELP_OPEN_FRAME[0:10]), [])
        self.assertEqual(p.on_client_data(LIBRELP_OPEN_FRAME[10:20]), [])
        self.assertEqual(p.on_client_data(LIBRELP_OPEN_FRAME[20:50]), [])
        self.assertEqual(p.on_client_data(LIBRELP_OPEN_FRAME[50:] + b'foobar'), [
            LIBRELP_OPEN_FRAME_PARSED,
            ])

    def test_two_frames(self):
        p = RelpFrameStreamingParser()

        self.assertEqual(p.on_client_data(LIBRELP_OPEN_FRAME * 2), [
            LIBRELP_OPEN_FRAME_PARSED,
            LIBRELP_OPEN_FRAME_PARSED,
            ])

    def test_empty_frame(self):
        p = RelpFrameStreamingParser()

        self.assertEqual(p.on_client_data(b'1 foo 0\n'), [
            RawRelpFrame(txid=1, cmd='foo', data=b'')
            ])

    def test_max_datalen(self):
        p = RelpFrameStreamingParser()

        with self.assertRaises(RelpParseError):
            self.assertEqual(p.on_client_data(b'1 open 1000000 '), [])

        p = RelpFrameStreamingParser()

        with self.assertRaises(RelpParseError):
            self.assertEqual(p.on_client_data(b'1 open 1000000\n'), [])

    def test_offers(self):
        self.assertEqual(parse_offers(b'''relp_version=0
relp_software=librelp,1.2.12,http://librelp.adiscon.com
commands=syslog\n'''), [
            (b'relp_version', b'0'),
            (b'relp_software', b'librelp,1.2.12,http://librelp.adiscon.com'),
            (b'commands', b'syslog'),
            ])

class TestRelpSerializer(TestCase):
    def test_frame(self):
        self.assertEqual(LIBRELP_OPEN_FRAME_PARSED.serialize(), LIBRELP_OPEN_FRAME)

    def test_empty_frame(self):
        p = RelpFrameStreamingParser()

        self.assertEqual(RawRelpFrame(txid=1, cmd='foo', data=b'').serialize(),
            b'1 foo 0\n')

    def test_offers(self):
        self.assertEqual(serialize_offers(0, [
            (b'relp_version', b'0'),
            (b'relp_software', b'librelp,1.2.12,http://librelp.adiscon.com'),
            (b'commands', b'syslog'),
            ]),
            b'''relp_version=0
relp_software=librelp,1.2.12,http://librelp.adiscon.com
commands=syslog''')

class TestSession(TestCase):
    def test_session(self):
        s = RelpSession()
        self.assertFalse(s.closed)
        s.on_client_data(LIBRELP_OPEN_FRAME)
        self.assertFalse(s.closed)
        self.assertEqual(b''.join(s.out_buffer), b'''1 rsp 62 200 OK
relp_version=0
relp_software=journalmon
commands=syslog\n''')
        self.assertEqual(s.messages, [])

        s.out_buffer = []

        s.on_client_data(b'''2 syslog 6 foobar\n''')
        self.assertEqual(s.out_buffer, [])
        self.assertEqual(s.messages, [(2, b'foobar')])

        s.ack_msg(2)
        self.assertEqual(b''.join(s.out_buffer), b'''2 rsp 6 200 OK\n''')
