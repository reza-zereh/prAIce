import pytest

from praice.utils.helpers import chunked


def test_chunked():
    # Test with a list of 10 items and chunk size of 3
    data = list(range(10))
    result = list(chunked(data, 3))
    assert result == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    # Test with an empty list
    assert list(chunked([], 5)) == []

    # Test with chunk size larger than the list
    assert list(chunked([1, 2, 3], 5)) == [[1, 2, 3]]

    # Test with chunk size of 1
    assert list(chunked([1, 2, 3], 1)) == [[1], [2], [3]]

    # Test with a string
    assert list(chunked("hello", 2)) == ["he", "ll", "o"]


@pytest.mark.parametrize(
    "iterable, chunk_size, expected",
    [
        ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
        (list(range(7)), 3, [[0, 1, 2], [3, 4, 5], [6]]),
        ("abcdefg", 3, ["abc", "def", "g"]),
    ],
)
def test_chunked_parametrized(iterable, chunk_size, expected):
    assert list(chunked(iterable, chunk_size)) == expected


def test_chunked_invalid_input():
    with pytest.raises(TypeError):
        list(chunked(None, 2))

    with pytest.raises(ValueError):
        list(chunked([1, 2, 3], 0))

    with pytest.raises(ValueError):
        list(chunked([1, 2, 3], -1))
