#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

from pimpamqueues import __version__


class TestTox(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import tox
        sys.exit(tox.cmdline(self.test_args))


class Setup(object):

    @staticmethod
    def long_description(filenames=['README.rst']):
        try:
            descriptions = []
            for filename in filenames:
                descriptions.append(open(filename).read())
            return "\n\n".join(descriptions)
        except:
            return ''

setup(
    name='pimpamqueues',
    version='%s' % (__version__, ),
    description='Lightweight queue interfaces with Redis super powers for '
                'distributed and non-distributed systems',
    long_description='%s' % (Setup.long_description(['README.rst',
                                                     'HISTORY.rst'])),
    author='Jordi Marín Valle',
    author_email='py.jordi@gmail.com',
    url='https://github.com/jordimarinvalle/pimpamqueues',
    license='MIT',
    platforms='all',
    packages=[
        'pimpamqueues',
    ],
    install_requires=[
        'redis',
    ],
    extras_require={
        'redis': ['redis', ],
        'testing': ['pytest', ],
    },
    tests_require=[
        'pytest',
    ],
    cmdclass={
        'test': TestTox,
    },
    test_suite="tests",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
    ],
    keywords=[
        'queue',
        'queues',
        'distributed system',
        'distributed systems',
        'redis',
        'lua',
    ],
)
