from typing import Union

import numpy as np
import torch
from numpy.linalg import norm


def cosine_similarity(
    a: Union[np.ndarray, torch.Tensor, list], b: Union[np.ndarray, torch.Tensor, list]
) -> float:
    """
    Calculate the cosine similarity between two vectors.

    Parameters:
        a (Union[np.ndarray, torch.Tensor, list]): The first vector.
        b (Union[np.ndarray, torch.Tensor, list]): The second vector.

    Returns:
        float: The cosine similarity between the two vectors.

    Raises:
        ValueError: If the input is not a list or numpy array.
        ValueError: If the input arrays do not have the same shape.
    """
    if isinstance(a, list) or isinstance(a, torch.Tensor):
        a = np.array(a)
    if isinstance(b, list) or isinstance(b, torch.Tensor):
        b = np.array(b)
    if not isinstance(a, np.ndarray) or not isinstance(b, np.ndarray):
        raise ValueError("Input must be a list, numpy array, or torch tensor")
    if a.shape != b.shape:
        raise ValueError("Input arrays must have the same shape")
    return float(np.dot(a, b) / (norm(a) * norm(b)))
