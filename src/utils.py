import math


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
