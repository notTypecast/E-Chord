import math
import json
from Finger import Finger

"""
File including various utility functions
"""


def create_request(header_dict, body_dict):
    """
    Creates request from passed header and body
    :param header_dict: dictionary of header
    :param body_dict: dictionary of body
    :return:
    """
    request_dict = {"header": header_dict, "body": body_dict}
    request_msg = json.dumps(request_dict, indent=2)

    return request_msg


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


def is_between_clockwise(x, lower, upper, inclusive_upper=False):
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


def place_in_successor_list(current_list, response_body, lower):
    """
    Places node with node_id found in response_body in current_list
    :param current_list: the list to place the new node in
    :param response_body: the dictionary containing ip, port and node_id of node
    :param lower: lower bound for check
    :return: None
    """
    new_node_id = response_body["node_id"]

    for i, succ in enumerate(current_list):
        if succ.node_id == new_node_id:
            break
        if is_between_clockwise(new_node_id, lower, succ.node_id):
            current_list.insert(i, Finger((response_body["ip"], response_body["port"]), new_node_id))
