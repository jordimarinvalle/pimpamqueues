#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from pimpamqueues import Tools


class TestTools(object):

    def setup(self):
        self.block_slices_300_1 = Tools.get_block_slices(300, 1)
        self.block_slices_10_10 = Tools.get_block_slices(10, 10)
        self.block_slices_27_10 = Tools.get_block_slices(27, 10)
        self.block_slices_13_2 = Tools.get_block_slices(13, 2)
        self.block_slices_3_141592 = Tools.get_block_slices(3, 141592)
        self.block_slices_0_1000 = Tools.get_block_slices(0, 1000)

    def test_block_slices(self):
        assert len(self.block_slices_300_1) == 300
        assert len(self.block_slices_10_10) == 1
        assert len(self.block_slices_27_10) == 3
        assert len(self.block_slices_13_2) == 7
        assert len(self.block_slices_3_141592) == 1
        assert len(self.block_slices_0_1000) == 1

    def test_block_slices_a_slice(self):
        assert self.block_slices_300_1[0] == [0, 1]
        assert self.block_slices_300_1[299] == [299, 300]
        assert self.block_slices_10_10[0] == [0, 10]
        assert self.block_slices_27_10[2] == [20, 30]
        assert self.block_slices_13_2[1] == [2, 4]
        assert self.block_slices_3_141592[0] == [0, 3]
        assert self.block_slices_0_1000[0] == [0, 0]


if __name__ == '__main__':
    pytest.main()
