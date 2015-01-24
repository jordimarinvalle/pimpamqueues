#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

from requeues import QUEUE_COLLECTION_OF_ELEMENTS
from requeues import KEEP_QUEUED_ELEMENTS_KEEP, KEEP_QUEUED_ELEMENTS_REMOVE

from requeues.exceptions import RequeuesError
from requeues.exceptions import RequeuesElementWithoutValueError
from requeues.exceptions import RequeuesDisambiguatorInvalidError

from requeues.simplequeue import SimpleQueue
from requeues.bucketqueue import BucketQueue


class SmartQueue(SimpleQueue, BucketQueue):
    '''
    A lightweight queue. Smart Queue. It only adds a unique element once
    for the queue's time living. If a element wants to be added more than once,
    queue will not be altered.
    '''

    QUEUE_TYPE_NAME = 'smart'

    def __init__(self, id_args=[],
                 collection_of=QUEUE_COLLECTION_OF_ELEMENTS,
                 keep_previous=KEEP_QUEUED_ELEMENTS_KEEP,
                 redis_conn=None, disambiguator=None):
        '''
        Create a SmartQueue object.

        Arguments:
        :id_args -- list, list's values will be used to name the queue
        :collection_of -- string (default: QUEUE_COLLECTION_OF_ELEMENTS),
                          a type descriptor of queued elements
        :keep_previous -- boolean (default: KEEP_QUEUED_ELEMENTS_KEEP),
                          a flag to create a fresh queue or not
        :redis_conn -- redis.client.Redis (default: None), a redis
                       connection will be created using the default
                       redis.client.Redis connection params.
        :disambiguator -- class (default: none), a class with a disambiguate
                          static method which receives a string as an argument.
                          It is used to discriminate those elements that
                          do not need to be pushed again.
        '''
        self.id_args = id_args
        self.collection_of = collection_of

        if disambiguator and not disambiguator.__dict__.get('disambiguate'):
            raise RequeuesDisambiguatorInvalidError()

        self.disambiguator = disambiguator

        if redis_conn is None:
            redis_conn = redis.Redis()
        self.redis = redis_conn

        self.key_queue = self.get_key_queue()
        self.key_queue_bucket = self.get_key_bucket()

        self.keys = [self.key_queue, self.key_queue_bucket, ]

        if keep_previous is KEEP_QUEUED_ELEMENTS_REMOVE:
            self.delete()

    def __str__(self):
        '''
        Return a string representation of the class.

        Returns: string
        '''
        return '<SmartQueue: %s (%s)>' % (self.key_queue, self.num())

    def push(self, element, queue_first=False):
        '''
        Push a element into the queue. Element can be pushed to the first or
        last position (by default is pushed to the last position).

        Arguments:
        :element -- string
        :queue_first -- boolean (default: False)

        Returns: long number, the number of elements that are in the queue
        '''
        if element in ('', None):
            raise RequeuesElementWithoutValueError()

        try:

            element = str(element)

            if not self._push_to_bucket(self.disambiguate(element)):
                return 0
            return self._push_to_queue(element, queue_first)

        except Exception:
            raise RequeuesError("%s was not pushed" % (element))

    def push_some(self, elements, queue_first=False, num_block_size=None):
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
        :queue_first -- boolean (default: false)
        :num_block_size -- integer (default: none)

        Raise:
        :RequeuesError(), if element can not be pushed

        Returns: long number, the number of elements that are in the queue
        '''
        try:

            elements = list(elements)

            disambiguated_elements = self.disambiguate_some(elements)
            add_statuses = self._push_some_to_bucket(disambiguated_elements, 1)

            elements_to_queue = []
            for i, status in enumerate(add_statuses):
                if status is 1:
                    elements_to_queue.append(elements[i])

            return self._push_some_to_queue(elements_to_queue, queue_first,
                                            num_block_size)

        except Exception as e:
            raise RequeuesError(e.message)

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
