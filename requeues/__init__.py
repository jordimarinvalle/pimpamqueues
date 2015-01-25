#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import hashlib


NUM_BLOCK_SIZE = 1000

QUEUE_COLLECTION_OF_URLS = 'urls'
QUEUE_COLLECTION_OF_JOBS = 'jobs'
QUEUE_COLLECTION_OF_TASKS = 'tasks'
QUEUE_COLLECTION_OF_ITEMS = 'items'
QUEUE_COLLECTION_OF_ELEMENTS = 'elements'


class DummyHasher(object):

    @staticmethod
    def hash(element):
        if element:
            return hashlib.md5(str(element).encode()).hexdigest()
        return ''


class Tools(object):

    @staticmethod
    def get_block_slices(num_elements, num_block_size=None):

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
