# -*- encoding: utf-8 -*-

from requeues import DummyHasher


class TestDummyHasher(object):

    ELEMENT = '42'

    ELEMENT_EMPTY_1 = ''
    ELEMENT_EMPTY_2 = None

    def setup(self):
        pass

    def test_hash_method(self):
        assert ('hash' in DummyHasher.__dict__) is True

    def test_hash_element_empty(self):
        assert DummyHasher.hash(self.ELEMENT_EMPTY_1) == ''
        assert DummyHasher.hash(self.ELEMENT_EMPTY_2) == ''

    def test_hash_element(self):
        assert DummyHasher.hash(self.ELEMENT) != self.ELEMENT


if __name__ == '__main__':
    pytest.main()
