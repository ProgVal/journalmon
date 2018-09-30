import random
from unittest import TestCase

from journalmon.collection_gateway.relp import *

LIBRELP_OPEN_FRAME = b'''1 open 86 relp_version=0
relp_software=librelp,1.2.12,http://librelp.adiscon.com
commands=syslog'''
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
