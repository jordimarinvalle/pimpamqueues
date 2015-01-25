#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from tests import redis_conn
from requeues.smartqueue import SmartQueue
from requeues.exceptions import RequeuesDisambiguatorInvalidError


ELEMENT_EGG = b'egg'
ELEMENT_BACON = b'bacon'
ELEMENT_SPAM = b'spam'
ELEMENT_42 = b'42'

ELEMENT_SPAM_UPPERCASED = b'SPAM'

some_elements = [
    ELEMENT_EGG,
    ELEMENT_BACON,
    ELEMENT_SPAM,
    ELEMENT_SPAM,
    ELEMENT_SPAM,
    ELEMENT_42,
    ELEMENT_SPAM,
    ELEMENT_SPAM_UPPERCASED,
]


class Disambiguator(object):

    @staticmethod
    def disambiguate(element):
        return str(element).lower()


class DisambiguatorInvalid(object):

    @staticmethod
    def invalid(element):
        return element.lower()


class TestSmartQueue(object):

    def setup(self):
        self.queue = SmartQueue(
            id_args=['test', 'testing'],
            redis_conn=redis_conn
        )

    def test_empty(self):
        assert self.queue.num() is 0
        assert self.queue.is_empty() is True
        assert self.queue.is_not_empty() is False

    def test_push(self):
        assert self.queue.push(ELEMENT_EGG) == 1

    def test_push_to_first(self):
        self.queue.push(ELEMENT_EGG)
        self.queue.push(ELEMENT_BACON)
        self.queue.push(ELEMENT_SPAM)
        self.queue.push(ELEMENT_42, to_first=True)
        assert self.queue.pop() == ELEMENT_42

    def test_push_some(self):
        assert self.queue.push_some(some_elements) == len(set(some_elements))

    def test_push_smart(self):
        self.queue.push(ELEMENT_EGG)
        self.queue.push(ELEMENT_BACON)
        self.queue.push(ELEMENT_SPAM)
        assert self.queue.push(ELEMENT_SPAM) == 0

    def test_push_some_to_first(self):
        self.queue.push(ELEMENT_42)
        self.queue.push_some(elements=[ELEMENT_EGG, ELEMENT_BACON],
                             to_first=True)
        assert self.queue.pop() == ELEMENT_EGG

    def test_pop(self):
        self.queue.push(ELEMENT_EGG)
        assert self.queue.pop() == ELEMENT_EGG

    def test_pop_none(self):
        assert self.queue.pop() is None

    def test_elements(self):
        self.queue.push_some(some_elements)
        elements = self.queue.elements()
        assert len(set(some_elements).difference(set(elements))) is 0

    def test_elements_first_elements(self):
        self.queue.push_some(some_elements)
        assert self.queue.first_elements(3) == some_elements[0:3]

    def test_disambiguate(self):
        self.queue = SmartQueue(
            id_args=['test', 'testing'],
            redis_conn=redis_conn,
            disambiguator=Disambiguator,
        )
        assert self.queue.push(ELEMENT_SPAM) == 1
        assert self.queue.push(ELEMENT_SPAM_UPPERCASED) == 0

    def test_disambiguate_some(self):
        self.queue = SmartQueue(
            id_args=['test', 'testing'],
            redis_conn=redis_conn,
            disambiguator=Disambiguator,
        )

        num_elements = self.queue.push_some(some_elements)
        assert num_elements == (len(set(some_elements)) - 1)
        assert self.queue.push(ELEMENT_SPAM_UPPERCASED) == 0

    def test_disambiguate_invalid(self):
        with pytest.raises(RequeuesDisambiguatorInvalidError):
            self.queue = SmartQueue(
                id_args=['test', 'testing'],
                redis_conn=redis_conn,
                disambiguator=DisambiguatorInvalid,
            )

    def test_delete(self):
        self.queue.push(element=ELEMENT_42)
        assert self.queue.num() == 1
        assert self.queue.delete() is True
        assert self.queue.num() == 0

    def test_queue_new_queue_remove_queued_elements(self):
        self.queue.push(ELEMENT_EGG)
        assert self.queue.is_not_empty() is True

        queue = SmartQueue(
            id_args=['test', 'testing'],
            keep_previous=False,
            redis_conn=redis_conn
        )
        assert queue.is_empty() is True

    def teardown(self):
        self.queue.delete()

if __name__ == '__main__':
    pytest.main()
