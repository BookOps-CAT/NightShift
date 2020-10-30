# -*- coding: utf-8 -*-

"""
Tests bot's NYPL Platform request methods
"""

import pytest

from nightshift.api_nyp import split_into_batches


@pytest.mark.parametrize(
    "arg,size,expectation",
    [
        (
            [1, 2, 3, 4, 5, 6, 7],
            2,
            [
                [1, 2],
                [3, 4],
                [5, 6],
                [7],
            ],
        ),
        ([1], 2, [[1]]),
        ([1, 2], 2, [[1, 2]]),
    ],
)
def test_split_into_batches(arg, size, expectation):
    assert split_into_batches(arg, size) == expectation


def test_split_into_batches_default_size():
    assert split_into_batches([1] * 125) == [[1] * 50, [1] * 50, [1] * 25]
