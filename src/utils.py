import math
import json
import threading
import logging
import socket
from os import environ
from src.Finger import Finger

"""
File including various utility functions and data
"""

EXPECTED_REQUEST = {
    "get_successor": (),
    "get_predecessor": (),
    "find_successor": ("for_id",),
    "get_closest_preceding_finger": ("for_key_id",),
    "get_prev_successor_list": (),
    "poll": (),
    "update_predecessor": ("ip", "port", "node_id"),
    "clear_predecessor": (),
    "batch_store_keys": ("keys",),
    "store_key": ("key", "value", "key_id"),
    "delete_key": ("key",),
    "lookup": ("key",),
    "find_key": ("key",),
    "find_and_store_key": ("key", "value"),
    "find_and_delete_key": ("key",),
    "debug_leave_ring": (),
    "debug_pred": (),
    "debug_succ_list": (),
    "debug_finger_table": (),
    "debug_storage": (),
    "debug_fail": (),
    "debug_get_total_keys": (),
}

# get configuration settings from params.json
with open("config/params.json") as f:
    params = json.load(f)

log = logging.getLogger(__name__)

log.info("Loaded params")


def get_ip():
    """
    Returns the local IP address of the machine
    :return: the IP address
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except (socket.error, socket.timeout):
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


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


def get_id(key, hash_func):
    """
    Returns ID of key
    :param key: string to hash and get ID
    :param hash_func: hash function to use
    :return: int ID corresponding to given key
    """
    key = key.encode("utf-8")

    ring_size = params["ring"]["bits"]
    # truncate to necessary number of bytes and get ID
    trunc_size = math.ceil(ring_size / 8)
    res_id = int.from_bytes(hash_func(key).digest()[-trunc_size:], "big")

    return res_id % 2**ring_size


def ask_peer(peer_addr, req_type, body_dict, custom_timeout=None):
    """
    Edited version of ask_peer for general use outside Node
    Sends a request and returns the response
    :param peer_addr: (IP, port) of peer
    :param req_type: type of request for request header
    :param body_dict: dictionary of body
    :param custom_timeout: timeout for request; network parameter is used if None
    :return: string response of peer
    """

    request_msg = create_request({"type": req_type}, body_dict)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(
            custom_timeout if custom_timeout is not None else params["net"]["timeout"]
        )
        try:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
            data = client.recv(params["net"]["data_size"]).decode()
        except (socket.error, socket.timeout):
            return None

    if not data:
        return None

    return json.loads(data)


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
    return (
        lower < upper and lower < x and (x <= upper if inclusive_upper else x < upper)
    ) or (
        upper <= lower and ((x <= upper if inclusive_upper else x < upper) or x > lower)
    )


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
            current_list.insert(
                i, Finger((response_body["ip"], response_body["port"]), new_node_id)
            )


def next_power_of_2(num):
    return 2 ** math.ceil(math.log(num, 2))


class RWLock:
    """
    Implements a readers-writer block with no priorities
    """

    def __init__(self):
        """
        Initializes two mutexes (reader and writer mutex) and the reader counter
        """
        self.r_lock = threading.Lock()
        self.w_lock = threading.Lock()
        self.readers = 0

    def r_enter(self):
        """
        Used for reader to enter critical region
        :return: None
        """
        self.r_lock.acquire()
        self.readers += 1
        if self.readers == 1:
            self.w_lock.acquire()
        self.r_lock.release()

    def r_leave(self):
        """
        Used for reader to leave critical region
        :return: None
        """
        self.r_lock.acquire()
        self.readers -= 1
        if not self.readers:
            self.w_lock.release()
        self.r_lock.release()

    def r_locked(self):
        return self.r_lock.locked()

    def w_enter(self):
        """
        Used for writer to enter critical region
        :return: None
        """
        self.w_lock.acquire()

    def w_leave(self):
        """
        Used for writer to leave critical region
        :return: None
        """
        self.w_lock.release()

    def w_locked(self):
        return self.w_lock.locked()
