#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from pimpamqueues import QUEUE_COLLECTION_OF_ELEMENTS

from pimpamqueues import Tools
from pimpamqueues.exceptions import PimPamQueuesError
from pimpamqueues.exceptions import PimPamQueuesElementWithoutValueError


class SimpleQueue(object):
    '''
    A lightweight queue. Simple Queue.
    '''

    QUEUE_TYPE_NAME = 'simple'

    def __init__(self, id_args, collection_of=QUEUE_COLLECTION_OF_ELEMENTS,
                 keep_previous=True, redis_conn=None):
        '''
        Create a SimpleQueue object.

        Arguments:
        :id_args -- list, list's values will be used to name the queue
        :collection_of -- string (default: QUEUE_COLLECTION_OF_ELEMENTS),
                          a type descriptor of queued elements
        :keep_previous -- boolean (default: true),
                          a flag to create a fresh queue or not
        :redis_conn -- redis.client.Redis (default: None), a redis
                       connection will be created using the default
                       redis.client.Redis connection params.
        '''
        self.id_args = id_args
        self.collection_of = collection_of

        if redis_conn is None:
            redis_conn = redis.Redis()
        self.redis = redis_conn

        self.key_queue = self.get_key_queue()

        if keep_previous is False:
            self.delete()

    def __str__(self):
        '''
        Return a string representation of the class.

        Returns: string
        '''
        return '<SimpleQueue: %s (%s)>' % (self.key_queue, self.num())

    def get_key_queue(self):
        '''
        Get a key id that will be used to store/retrieve data from
        the redis server.

        Returns: string
        '''
        return 'queue:%s:type:%s:of:%s' % ('.'.join(self.id_args),
                                           SimpleQueue.QUEUE_TYPE_NAME,
                                           self.collection_of)

    def push(self, element, to_first=False):
        '''
        Push a element into the queue. Element can be pushed to the first or
        last position (by default is pushed to the last position).

        Arguments:
        :element -- string
        :to_first -- boolean (default: False)

        Raise:
        :PimPamQueuesElementWithoutValueError, if element has not a value

        Returns: long, the number of queued elements
        '''
        if element in ('', None):
            raise PimPamQueuesElementWithoutValueError()
        return self.push_some([element, ], to_first)

    def push_some(self, elements, to_first=False, num_block_size=None):
        '''
        Push a bunch of elements into the queue. Elements can be pushed to the
        first or last position (by default are pushed to the last position).

        Arguments:
        :elements -- a collection of strings
        :to_first -- boolean (default: false)
        :num_block_size -- integer (default: none)

        Returns: long, the number of queued elements
        '''
        try:

            elements = list(elements)

            if to_first:
                elements.reverse()

            block_slices = Tools.get_block_slices(
                num_elements=len(elements),
                num_block_size=num_block_size
            )

            pipe = self.redis.pipeline()
            for s in block_slices:
                some_elements = elements[s[0]:s[1]]
                if to_first:
                    pipe.lpush(self.key_queue, *some_elements)
                else:
                    pipe.rpush(self.key_queue, *some_elements)
            return pipe.execute().pop()

        except Exception as e:
            raise PimPamQueuesError(e.message)

    def pop(self, last=False):
        '''
        Pop a element from the queue. Element can be popped from the begining
        or the ending of the queue (by default pops from the begining).

        If no element is poped, it returns None

        Arguments:
        :last -- boolean (default: false)

        Returns: string, the popped element, or, none, if no element is popped
        '''
        if last:
            return self.redis.rpop(self.key_queue)
        return self.redis.lpop(self.key_queue)

    def num(self):
        '''
        Get the number of elements that are queued.

        Returns: integer, the number of elements that are queued
        '''
        return self.redis.llen(self.key_queue)

    def is_empty(self):
        '''
        Check if the queue is empty.

        Returns: boolean, true if queue is empty, otherwise false
        '''
        return True if self.num() is 0 else False

    def is_not_empty(self):
        '''
        Check if the queue is not empty.

        Returns: boolean, true if queue is not empty, otherwise false
        '''
        return not self.is_empty()

    def elements(self, queue_from=0, queue_to=-1):
        '''
        Get some (or even all) queued elements, by the order that they are
        queued. By default it returns all queued elements.

        Note
        ====
        Elements are not popped.

        Arguments:
        :queue_from -- integer (default: 0)
        :queue_to -- integer (default: -1)

        Returns: list
        '''
        return self.redis.lrange(self.key_queue, queue_from, queue_to)

    def first_elements(self, num_elements=10):
        '''
        Get the N first queued elements, by the order that they are
        queued. By default it returns the first ten elements.

        Note
        ====
        Elements are not popped.

        Arguments:
        :num_elements -- integer (default: 10)

        Returns: list
        '''
        queue_to = num_elements - 1
        return self.elements(queue_to=queue_to)

    def remove(self, element):
        '''
        Remove a element from the queue.

        Arguments:
        :element -- string

        Returns: boolean, return true if element was removed, otherwise false
        '''
        return True if self.redis.lrem(self.key_queue, element) else False

    def delete(self):
        '''
        Delete the queue with all its elements.

        Returns: boolean, true if queue has been deleted, otherwise false
        '''
        return True if self.redis.delete(self.key_queue) else False
