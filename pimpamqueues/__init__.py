#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math


NUM_BLOCK_SIZE = 1000

QUEUE_COLLECTION_OF_URLS = 'urls'
QUEUE_COLLECTION_OF_JOBS = 'jobs'
QUEUE_COLLECTION_OF_TASKS = 'tasks'
QUEUE_COLLECTION_OF_ITEMS = 'items'
QUEUE_COLLECTION_OF_ELEMENTS = 'elements'

VERSION_MAJOR = 1
VERSION_MINOR = 0
VERSION_MICRO = 2

__version__ = "%s" % (".".join(str(v) for v in [VERSION_MAJOR, VERSION_MINOR,
                      VERSION_MICRO]))


class Tools(object):

    @staticmethod
    def get_block_slices(num_elements, num_block_size=None):
        '''
        Get how many loops and for each loop the position from and to for a
        bunch of elements that are going to be pushed. It is useful for big
        amount of element to be pepelined to the Redis server.

        Arguments:
        :num_elements -- integer, number of elements that are going to
                         be pushed
        :num_block_size -- integer (default: none), how big are going to be
                           the Redis pipeline blocks

        Returns: list of lists
        '''

        block_slices = []

        if num_block_size is None:
            num_block_size = NUM_BLOCK_SIZE

        if num_block_size > num_elements or num_elements is 0:
            return [[0, num_elements]]

        num_loops = int(math.ceil(num_elements / float(num_block_size)))
        for i in range(0, num_loops):
            position_from = i * num_block_size
            position_to = position_from + num_block_size
            block_slices.append([position_from, position_to])

        return block_slices
