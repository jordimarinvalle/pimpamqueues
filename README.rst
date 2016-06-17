PimPamQueues
============

**Lightweight queue interfaces with Redis super powers**

.. image:: https://api.travis-ci.org/jordimarinvalle/pimpamqueues.svg
    :target: https://travis-ci.org/jordimarinvalle/pimpamqueues
    :alt: Build status

.. image:: https://img.shields.io/pypi/v/pimpamqueues.svg
    :target: https://pypi.python.org/pypi/pimpamqueues/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/pimpamqueues.svg
    :target: https://pypi.python.org/pypi/pimpamqueues/
    :alt: Supported Python versions


Description
-----------
PimPamQueues provides easy and lightweight Python interfaces to interact with queues on a distributed system.


Requirements
------------
- Python 2.7, 3.4 or 3.5 `python.org <https://www.python.org/>`_
- A running Redis server, `redis.io <http://redis.io/>`_
- Redis Python library, `redis-py.readthedocs.org <https://redis-py.readthedocs.org/en/latest/>`_


Features
--------
- Supports Python 2.7, 3.4 and 3.5.
- Provides super-simple queue interfaces for creating different types of queues.
- Designed to be used on distributed systems - also works on non-distributed systems 😎.


Queue Interfaces
----------------
- SimpleQueue, just a regular queue.
- BucketQueue, unordered queue of unique elements with a extremely fast element existence search method.
- SmartQueue, queue which stores queued elements aside the queue for not queueing the same incoming elements again.


Installation
------------

PIP
~~~

For a pip installation, just ``pip install pimpamqueues``

.. code:: bash

    $ pip install pimpamqueues



Usage
-----

SimpleQueue
~~~~~~~~~~~

.. code:: bash

    >>> from pimpamqueues.simplequeue import SimpleQueue
    >>> queue = SimpleQueue(id_args=['simplequeue'])
    >>> queue.num()
    0
    >>> queue.push('egg')
    1
    >>> queue.push_some(['bacon', 'spam'])
    3
    >>> queue.num()
    3
    >>> queue.pop()
    b'egg'
    >>> queue.is_empty()
    False
    >>> queue.push('spam', to_first=True)
    3
    >>> queue.elements()
    [b'spam', b'bacon', b'spam']
    >>> queue.pop()
    b'spam'
    >>> queue.elements()
    [b'bacon', b'spam']
    ...


BucketQueue
~~~~~~~~~~~

.. code:: bash

    >>> from pimpamqueues.bucketqueue import BucketQueue
    >>> queue = BucketQueue(id_args=['bucketqueue'])
    >>> queue.num()
    0
    >>> queue.push('egg')
    'egg'
    >>> queue.push_some(['bacon', 'spam'])
    [b'bacon', b'spam']
    >>> queue.num()
    3
    >>> queue.pop()
    b'spam'
    >>> queue.is_empty()
    False
    >>> queue.push('spam')
    'spam'
    >>> queue.elements()
    {b'bacon', b'egg', b'spam'}
    >>> queue.pop()
    b'spam'
    >>> queue.elements()
    {b'bacon', b'egg'}
    ...


SmartQueue
~~~~~~~~~~

.. code:: bash

    >>> from pimpamqueues.smartqueue import SmartQueue
    >>> queue = SmartQueue(id_args=['smartqueue'])
    >>> queue.num()
    0
    >>> queue.push('egg')
    'egg'
    >>> queue.push_some(['bacon', 'spam'])
    [b'bacon', b'spam']
    >>> queue.num()
    3
    >>> queue.pop()
    b'egg'
    >>> queue.is_empty()
    False
    >>> queue.push('spam', to_first=True)
    ''
    >>> queue.elements()
    [b'bacon', b'spam']
    >>> queue.pop()
    b'bacon'
    >>> queue.elements()
    [b'spam']
    >>> queue.push('spam', force=True)
    'spam'
    >>> queue.push_some(['spam', 'spam'], force=True)
    [b'spam', b'spam']
    >>> queue.elements()
    [b'spam', b'spam', b'spam', b'spam']
    ...
