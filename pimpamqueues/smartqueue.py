#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from pimpamqueues import QUEUE_COLLECTION_OF_ELEMENTS

from pimpamqueues import Tools
from pimpamqueues.simplequeue import SimpleQueue
from pimpamqueues.bucketqueue import BucketQueue

from pimpamqueues.exceptions import PimPamQueuesError
from pimpamqueues.exceptions import PimPamQueuesElementWithoutValueError
from pimpamqueues.exceptions import PimPamQueuesDisambiguatorInvalidError


class SmartQueue(SimpleQueue, BucketQueue):
    '''
    A lightweight queue. Smart Queue. It only adds a unique element once
    for the queue's time living. If a element wants to be added more than once,
    queue will not be altered.
    '''

    QUEUE_TYPE_NAME = 'smart'

    def __init__(self, id_args, collection_of=QUEUE_COLLECTION_OF_ELEMENTS,
                 keep_previous=True, redis_conn=None, disambiguator=None):
        '''
        Create a SmartQueue object.

        Arguments:
        :id_args -- list, list's values will be used to name the queue
        :collection_of -- string (default: QUEUE_COLLECTION_OF_ELEMENTS),
                          a type descriptor of queued elements
        :keep_previous -- boolean (default: true),
                          a flag to create a fresh queue or not
        :redis_conn -- redis.client.Redis (default: None), a redis
                       connection will be created using the default
                       redis.client.Redis connection params.
        :disambiguator -- class (default: none), a class with a disambiguate
                          static method which receives a string as an argument
                          and return a string. It is used to discriminate
                          those elements that do not need to be pushed again.

        Raise:
        :PimPamQueuesDisambiguatorInvalidError(), if disambiguator argument
                                                  is invalid
        '''
        self.id_args = id_args
        self.collection_of = collection_of

        if disambiguator and not disambiguator.__dict__.get('disambiguate'):
            raise PimPamQueuesDisambiguatorInvalidError()

        self.disambiguator = disambiguator

        if redis_conn is None:
            redis_conn = redis.Redis()
        self.redis = redis_conn

        self.key_queue = self.get_key_queue()
        self.key_queue_bucket = self.get_key_bucket()

        self.keys = [self.key_queue, self.key_queue_bucket, ]

        if keep_previous is False:
            self.delete()

    def __str__(self):
        '''
        Return a string representation of the class.

        Returns: string
        '''
        return '<SmartQueue: %s (%s)>' % (self.key_queue, self.num())

    def push(self, element, to_first=False, force=False):
        '''
        Push a element into the queue. Element can be pushed to the first or
        last position (by default is pushed to the last position).

        Arguments:
        :element -- string
        :to_first -- boolean (default: False)
        :force -- boolean (default: False)

        Raise:
        :PimPamQueuesError(), if element can not be pushed
        :PimPamQueuesElementWithoutValueError, if element has not a value

        Returns: string, if element was queued returns the queued element,
                 otherwise, empty string
        '''
        if element in ('', None):
            raise PimPamQueuesElementWithoutValueError()

        try:
            if self.push_some([element, ], to_first, force):
                return element
            return ''
        except Exception:
            raise PimPamQueuesError("%s was not pushed" % (element))

    def push_some(self, elements, to_first=False, force=False,
                  num_block_size=None):
        '''
        Push a bunch of elements into the queue. Elements can be pushed to the
        first or last position (by default are pushed to the last position).

        Arguments:
        :elements -- a collection of strings
        :to_first -- boolean (default: false)
        :force -- boolean (default: False)
        :num_block_size -- integer (default: none)

        Raise:
        :PimPamQueuesError(), if element can not be pushed

        Returns: list of strings, a list with queued elements
        '''
        try:

            elements = self.disambiguate_some(list(elements))

            if to_first:
                elements.reverse()

            block_slices = Tools.get_block_slices(
                num_elements=len(elements),
                num_block_size=num_block_size
            )

            queued_elements = []
            for s in block_slices:
                some_elements = self.__push_some(
                    elements=elements[s[0]:s[1]],
                    to_first=to_first,
                    force=force
                )
                queued_elements.extend(some_elements)
            return queued_elements

        except Exception as e:
            raise PimPamQueuesError(e.message)

    def disambiguate(self, element):
        '''
        Treats a element.

        Arguments:
        :element -- string

        Returns: string
        '''
        if self.__has_to_disambiguate():
            return self.disambiguator.disambiguate(element)
        return element

    def disambiguate_some(self, elements):
        '''
        Treats a list of elements.

        Arguments:
        :elements -- elements

        Returns: list of strings
        '''
        if self.__has_to_disambiguate():
            return [self.disambiguate(element) for element in elements]
        return elements

    def delete(self):
        '''
        Delete the queue with all its elements.

        Returns: boolean, true if queue has been deleted, otherwise false
        '''
        pipe = self.redis.pipeline()
        for key in self.keys:
            pipe.delete(key)
        return True if len(pipe.execute()) is len(self.keys) else False

    def __has_to_disambiguate(self):
        '''
        Check if disambiguation code has to be triggered.

        Returns: boolean, true if queue needs to disambiguate, otherwise false
        '''
        return True if self.disambiguator else False

    def __push_some(self, elements, to_first=False, force=False):
        '''
        Push some elements into the queue. Elements can be pushed to the
        first or last position (by default are pushed to the last position).

        Arguments:
        :elements -- a collection of strings
        :to_first -- boolean (default: false)
        :force -- boolean (default: False)

        Returns: list of strings, a list with queued elements
        '''
        push_to = 'lpush' if to_first is True else 'rpush'

        keys = [self.key_queue_bucket, self.key_queue, push_to]
        return self.redis.eval(self.__lua_push(force), len(keys),
                               *(keys + elements))

    def __lua_push(self, force=False):
        if force:
            return """
                local elements = {}

                for i=1, #ARGV do
                  redis.call('SADD', KEYS[1], ARGV[i])
                  table.insert(elements, ARGV[i])
                end

                for i=1, #elements do
                  redis.call(KEYS[3], KEYS[2], elements[i])
                end

                return elements
            """

        return """
            local elements = {}

            for i=1, #ARGV do
              if redis.call('SADD', KEYS[1], ARGV[i]) == 1 then
                table.insert(elements, ARGV[i])
              end
            end

            for i=1, #elements do
              redis.call(KEYS[3], KEYS[2], elements[i])
            end

            return elements
        """
