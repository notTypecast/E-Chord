import math

"""
File including various utility functions
"""

def get_id(key, hash_func, params):
    """
    Returns ID of key
    :param key: string to hash and get ID
    :param hash_func: hash function to use
    :param params: params JSON object
    :return: int ID corresponding to given key
    """
    key = key.encode("utf-8")

    ring_size = params["ring"]["size"]
    # truncate to necessary number of bytes and get ID
    trunc_size = math.ceil(ring_size/8)
    res_id = int.from_bytes(hash_func(key).digest()[-trunc_size:], "big")

    return res_id % 2**ring_size


def is_between_clockwise(x, lower, upper, inclusive_upper = False):
    """
    Checks if x is between lower and upper in a circle, while moving clockwise from lower to upper
    Lower is exclusive
    :param x: the value to check for
    :param lower: lower bound
    :param upper: upper bound
    :param inclusive_upper: determines if upper should be inclusive or not
    :return: True or False
    """
    return (lower < upper and lower < x  and (x <= upper if inclusive_upper else x < upper)) or \
           (upper <= lower and ((x <= upper if inclusive_upper else x < upper) or x > lower))
