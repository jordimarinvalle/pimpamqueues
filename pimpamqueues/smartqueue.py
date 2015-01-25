#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from pimpamqueues import QUEUE_COLLECTION_OF_ELEMENTS

from pimpamqueues.exceptions import PimPamQueuesError
from pimpamqueues.exceptions import PimPamQueuesElementWithoutValueError
from pimpamqueues.exceptions import PimPamQueuesDisambiguatorInvalidError

from pimpamqueues.simplequeue import SimpleQueue
from pimpamqueues.bucketqueue import BucketQueue


class SmartQueue(SimpleQueue, BucketQueue):
    '''
    A lightweight queue. Smart Queue. It only adds a unique element once
    for the queue's time living. If a element wants to be added more than once,
    queue will not be altered.
    '''

    QUEUE_TYPE_NAME = 'smart'

    def __init__(self, id_args=[], collection_of=QUEUE_COLLECTION_OF_ELEMENTS,
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

    def push(self, element, to_first=False):
        '''
        Push a element into the queue. Element can be pushed to the first or
        last position (by default is pushed to the last position).

        Arguments:
        :element -- string
        :to_first -- boolean (default: False)

        Raise:
        :PimPamQueuesError(), if element can not be pushed
        :PimPamQueuesElementWithoutValueError, if element has not a value

        Returns: string, if element was queued returns the queued element,
                 otherwise, empty string
        '''
        if element in ('', None):
            raise PimPamQueuesElementWithoutValueError()

        try:

            element = str(element)

            if not self._push_to_bucket(self.disambiguate(element)):
                return ''

            return element if self._push_to_queue(element, to_first) else ''

        except Exception:
            raise PimPamQueuesError("%s was not pushed" % (element))

    def push_some(self, elements, to_first=False, num_block_size=None):
        '''
        Push a bunch of elements into the queue. Elements can be pushed to the
        first or last position (by default are pushed to the last position).

        Note
        ====
        As Redis SADD function's response returns the number of added elements
        and not the added elements, there is a call to Redis server for each
        element to check if it was previously added or not. It is required to
        know it for pushing only to the queue those elements that have not been
        pushed to the queue in the queue time living.

        Arguments:
        :elements -- a collection of strings
        :to_first -- boolean (default: false)
        :num_block_size -- integer (default: none)

        Raise:
        :PimPamQueuesError(), if element can not be pushed

        Returns: list of strings, a list with queued elements
        '''
        try:

            elements = list(elements)

            disambiguated_elements = self.disambiguate_some(elements)
            add_statuses = self._push_some_to_bucket(disambiguated_elements, 1)

            elements_to_queue = []
            for i, status in enumerate(add_statuses):
                if status is 1:
                    elements_to_queue.append(elements[i])

            self._push_some_to_queue(elements_to_queue, to_first,
                                     num_block_size)

            return elements_to_queue

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
