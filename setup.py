#!/usr/bin/env python3

from distutils.core import setup

setup(
        name='journalmon',
        version='0.0.1',
        description='A journal-based monitoring daemon',
        author='Valentin Lorentz',
        author_email='progval+git@progval.net',
        url='https://github.com/ProgVal/journalmon',
        packages=[
            'journalmon',
            'journalmon.journals',
            'journalmon.storage_backends',
            'journalmon.collection_gateway',
        ],
        )
