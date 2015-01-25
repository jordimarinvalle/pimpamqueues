#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from tests import redis_conn
from requeues.bucketqueue import BucketQueue


ELEMENT_EGG = b'egg'
ELEMENT_BACON = b'bacon'
ELEMENT_SPAM = b'spam'
ELEMENT_42 = b'42'

ELEMENT_UNEXISTENT_ELEMENT = b'utopia'

some_elements = [
    ELEMENT_EGG,
    ELEMENT_BACON,
    ELEMENT_SPAM,
    ELEMENT_SPAM,
    ELEMENT_SPAM,
    ELEMENT_42,
    ELEMENT_SPAM,
]


class TestBucketQueue(object):

    def setup(self):
        self.queue = BucketQueue(
            id_args=['test', 'testing'],
            redis_conn=redis_conn
        )

    def test_empty(self):
        assert self.queue.num() is 0
        assert self.queue.is_empty() is True
        assert self.queue.is_not_empty() is False

    def test_push(self):
        assert self.queue.push(ELEMENT_EGG) is 1

    def test_push_some(self):
        push_statuses = self.queue.push_some(some_elements, 1)
        num_pushed = 0
        for status in push_statuses:
            num_pushed += status
        assert num_pushed <= len(some_elements)

    def test_pop(self):
        self.queue.push_some(some_elements)
        assert self.queue.pop() != None

    def test_pop_empty_queue(self):
        assert self.queue.pop() is None

    def test_is_element(self):
        self.queue.push_some(some_elements)
        assert self.queue.is_element(some_elements[0]) is True

    def test_is_not_element(self):
        self.queue.push_some(some_elements)
        assert self.queue.is_element(ELEMENT_UNEXISTENT_ELEMENT) is False

    def test_elements(self):
        self.queue.push_some(some_elements)
        elements = self.queue.elements()
        for i, some_element in enumerate(list(set(some_elements))):
            elements.discard(some_element)
        assert len(elements) is 0

    def test_n_elements(self):
        self.queue.push_some(some_elements)
        num_remaining = 1
        elements = self.queue.elements(len(set(some_elements)) - num_remaining)
        assert len(set(some_elements).difference(elements)) is num_remaining

    def test_fresh_queue(self):
        self.queue.push(ELEMENT_EGG)
        assert self.queue.is_not_empty() is True

        queue_y = BucketQueue(
            id_args=['test', 'testing'],
            keep_previous=False,
            redis_conn=redis_conn
        )
        assert queue_y.is_empty() is True

    def test_delete(self):
        self.queue.push(element=ELEMENT_42)
        assert self.queue.num() == 1
        assert self.queue.delete() is True
        assert self.queue.num() == 0

    def teardown(self):
        self.queue.delete()


if __name__ == '__main__':
    pytest.main()
