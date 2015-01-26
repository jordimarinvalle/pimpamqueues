#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from pimpamqueues import QUEUE_COLLECTION_OF_ELEMENTS

from pimpamqueues import Tools
from pimpamqueues.exceptions import PimPamQueuesError
from pimpamqueues.exceptions import PimPamQueuesElementWithoutValueError


class BucketQueue(object):
    '''
    A lightweight queue. Bucket Queue, uniqueness and super-fast queue
    element checking.
    '''

    QUEUE_TYPE_NAME = 'bucket'

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

        self.key_queue_bucket = self.get_key_bucket()

        if keep_previous is False:
            self.delete()

    def __str__(self):
        '''
        Return a string representation of the class.

        Returns: string
        '''
        return '<BucketQueue: %s (%s)>' % (self.key_queue_bucket, self.num())

    def get_key_bucket(self):
        '''
        Get a key id that will be used to store/retrieve data from
        the redis server.

        Returns: string
        '''
        return 'queue:%s:type:%s:of:%s' % ('.'.join(self.id_args),
                                           BucketQueue.QUEUE_TYPE_NAME,
                                           self.collection_of)

    def push(self, element):
        '''
        Push a element into the queue.

        Arguments:
        :element -- string

        Returns: string, element if element was queued otherwise a empty string
        '''
        if element in ('', None):
            raise PimPamQueuesElementWithoutValueError()
        return element if self.push_some([element, ]) else ''

    def push_some(self, elements, num_block_size=None):
        '''
        Push a bunch of elements into the queue.

        Arguments:
        :elements -- a collection of strings
        :num_block_size -- integer (default: none)

        Returns: list of strings, list of queued elements
        '''
        try:

            elements = list(elements)

            block_slices = Tools.get_block_slices(
                num_elements=len(elements),
                num_block_size=num_block_size
            )

            queued_elements = []
            for s in block_slices:
                queued_elements.extend(self.__push_some(elements[s[0]:s[1]]))
            return queued_elements

        except Exception as e:
            raise PimPamQueuesError(e.message)

    def pop(self):
        '''
        Pop a random element from the queue.

        If no element is poped, it returns None

        Arguments:
        :last -- boolean (default: false)

        Returns: string, the popped element, or, none, if no element is popped
        '''
        return self.redis.spop(self.key_queue_bucket)

    def num(self):
        '''
        Get the number of elements that are queued.

        Returns: integer, the number of elements that are queued
        '''
        return self.redis.scard(self.key_queue_bucket)

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

    def is_element(self, element):
        '''
        Checks if a element is in the queue. It returns true is element is in
        the queue, otherwise false.

        Arguments:
        :element -- string

        Returns: boolean
        '''
        return self.redis.sismember(self.key_queue_bucket, element)

    def elements(self, num_elements=-1):
        '''
        Get some (or even all) unordered queued elements.
        By default it returns all queued elements.

        Note
        ====
        Elements are not popped.

        Arguments:
        :num_elements -- integer (default: -1).

        Returns: set
        '''
        if num_elements is -1:
            return self.redis.smembers(self.key_queue_bucket)
        return set(self.redis.srandmember(self.key_queue_bucket, num_elements))

    def delete(self):
        '''
        Delete the queue with all its elements.

        Returns: boolean, true if queue has been deleted, otherwise false
        '''
        return True if self.redis.delete(self.key_queue_bucket) else False

    def __push_some(self, elements):
        '''
        Push some elements into the queue.

        Arguments:
        :elements -- a collection of strings

        Returns: list of strings, a list with queued elements
        '''
        keys = [self.key_queue_bucket, ]
        return self.redis.eval(self.__lua_push(), len(keys),
                               *(keys + elements))

    def __lua_push(self):
        return """
            local elements = {}

            for i=1, #ARGV do
              if redis.call('SADD', KEYS[1], ARGV[i]) == 1 then
                table.insert(elements, ARGV[i])
              end
            end

            return elements
        """
