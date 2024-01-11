from src import utils
from src.Finger import Finger

"""
rpc_handlers.py
| Contains functions which handle remote procedure calls
| Each request type is mapped to its corresponding function in REQUEST_MAP
| The request handler thread calls the corresponding function using the request type found in the header
| For functions that require writing to the node object on which the remote procedure is called, a function which 
accepts the node object as an argument and writes the changes is added to the queue; the main thread will then
remove the callable object from the queue and call it, writing the changes
"""

STATUS_OK = 200
STATUS_NOT_FOUND = 404
STATUS_CONFLICT = 409

# Map RPC types with handlers
REQUEST_MAP = {
    "get_successor": lambda n, body: get_successor(n),
    "get_predecessor": lambda n, body: get_predecessor(n),
    "find_successor": lambda n, body: find_successor(n, body),
    "get_closest_preceding_finger": lambda n, body: get_closest_preceding_finger(
        n, body
    ),
    "get_prev_successor_list": lambda n, body: get_prev_successor_list(n),
    "poll": lambda n, body: poll(),
    "update_predecessor": lambda n, body: update_predecessor(n, body),
    "clear_predecessor": lambda n, body: clear_predecessor(n),
    "batch_store_keys": lambda n, body: batch_store_keys(n, body),
    "store_key": lambda n, body: store_key(n, body),
    "delete_key": lambda n, body: delete_key(n, body),
    "lookup": lambda n, body: lookup(n, body),
    "find_key": lambda n, body: find_key(n, body),
    "find_and_store_key": lambda n, body: find_and_store_key(n, body),
    "find_and_delete_key": lambda n, body: find_and_delete_key(n, body),
    "debug_leave_ring": lambda n, body: debug_leave_ring(n),
    "debug_pred": lambda n, body: debug_pred(n),
    "debug_succ_list": lambda n, body: debug_succ_list(n),
    "debug_finger_table": lambda n, body: debug_finger_table(n),
    "debug_storage": lambda n, body: debug_storage(n),
    "debug_fail": lambda n, body: debug_fail(n),
    "debug_get_total_keys": lambda n, body: debug_get_total_keys(n),
}


def debug_leave_ring(n):
    """
    Tells node n to leave the ring
    :param n: the node
    :return: None
    """

    def leave(node):
        node.leaving = True

    n.event_queue.put(1)
    n.event_queue.put(leave)

    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def debug_pred(n):
    """
    Prints predecessor
    :param n: node whose predecessor to print
    :return: None
    """
    print("--------------------------------")
    print(f"My node ID is: {n.node_id}")
    if n.predecessor:
        print("Predecessor is: ")
        print(f"Addr: {n.predecessor.addr} with ID: {n.predecessor.node_id}")
    else:
        print("There is no predecessor")
    print("--------------------------------")
    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def debug_succ_list(n):
    """
    Prints successor list
    :param n: node whose successor list to print
    :return: None
    """
    print("--------------------------------")
    print(f"My node ID is: {n.node_id}")
    print("Successor list is:")
    for i, succ in enumerate(n.successor_list):
        print(f"{i} Addr: {succ.addr} with ID: {succ.node_id}")
    print("--------------------------------")
    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def debug_finger_table(n):
    """
    Prints finger table
    :param n: node whose table to print
    :return: None
    """
    print("--------------------------------")
    print(f"My node ID is: {n.node_id}")
    print("Finger table is:")
    for i, succ in enumerate(n.finger_table):
        print(f"{i} Addr: {succ.addr} with ID: {succ.node_id}")
    print("--------------------------------")
    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def debug_storage(n):
    """
    Prints keys and values stored in node n
    :param n:
    :return:
    """

    print("--------------------------------")
    print(f"My node ID is: {n.node_id}")
    print("Storage content is:")
    print(n.storage)
    print("--------------------------------")
    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def debug_fail(n):
    """
    Closes the node without notifying other nodes, to simulate failrue
    :return: None
    """
    n.event_queue.put(lambda node: exit(0))


def debug_get_total_keys(n):
    """
    Returns response to get_total_keys remote procedure call
    :param n: node whose total keys to return
    :return: string of reponse
    """
    resp_header = {"status": STATUS_OK}
    resp_body = {"total_keys": len(n.storage)}

    return utils.create_request(resp_header, resp_body)


# Functions that only read from node object n
def get_successor(n):
    """
    Returns response to get_successor remote procedure call
    :param n: node whose successor to return
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}
    resp_body = {
        "ip": n.finger_table[0].addr[0],
        "port": n.finger_table[0].addr[1],
        "node_id": n.finger_table[0].node_id,
    }

    return utils.create_request(resp_header, resp_body)


def get_predecessor(n):
    """
    Returns response to get_predecessor remote procedure call
    :param n: node whose predecessor to return
    :return: string of response
    """
    resp_header = {}
    resp_body = {}
    if n.predecessor:
        resp_header["status"] = STATUS_OK
        resp_body["ip"] = n.predecessor.addr[0]
        resp_body["port"] = n.predecessor.addr[1]
        resp_body["node_id"] = n.predecessor.node_id
    else:
        resp_header["status"] = STATUS_NOT_FOUND

    return utils.create_request(resp_header, resp_body)


def find_successor(n, body):
    """
    Returns response to find_successor remote procedure call
    :param n: node on which to call find_successor method
    :param body: body of request
    :return: string of response
    """
    successor_data = n.find_successor(body["for_id"])

    resp_header = {}
    resp_body = {}
    if successor_data:
        resp_header["status"] = STATUS_OK
        resp_body["ip"] = successor_data[0]
        resp_body["port"] = successor_data[1]
        resp_body["node_id"] = successor_data[2]
    # look up failed
    else:
        resp_header["status"] = STATUS_NOT_FOUND

    return utils.create_request(resp_header, resp_body)


def get_closest_preceding_finger(n, body):
    """
    Returns response to get_closest_preceding_finger remote procedure call
    :param n: node on which to call closest_preceding_finger
    :param body: body of request
    :return: string of response
    """
    fingers = n.closest_preceding_finger(body["for_key_id"])

    resp_header = {"status": STATUS_OK}
    # return fingers and if successor of current node is among them
    resp_body = {"fingers": [], "contains_successor": n.finger_table[0] in fingers}
    for finger in fingers:
        resp_body["fingers"].append(
            {"ip": finger.addr[0], "port": finger.addr[1], "node_id": finger.node_id}
        )

    return utils.create_request(resp_header, resp_body)


def get_prev_successor_list(n):
    """
    Returns successor list for previous node
    Successor list will contain n's successor as first entry, and all nodes in n's successor list up to r-1
    :param n: the node
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}
    prev_successor_list = [
        {
            "ip": n.finger_table[0].addr[0],
            "port": n.finger_table[0].addr[1],
            "node_id": n.finger_table[0].node_id,
        }
    ]
    for succ in n.successor_list[:-1]:
        prev_successor_list.append(
            {"ip": succ.addr[0], "port": succ.addr[1], "node_id": succ.node_id}
        )
    resp_body = {"successor_list": prev_successor_list}

    return utils.create_request(resp_header, resp_body)


def poll():
    """
    Reads poll request from seed server, responds with OK
    :return: string of response
    """
    resp_header = {"status": STATUS_OK}

    return utils.create_request(resp_header, {})


def find_key(n, body):
    """
    Looks through chord for node with key and returns its value
    :param n: the node which should call find_key
    :param body: the request body
    :return: string of response
    """
    value = n.find_key(body["key"])

    resp_header = {}
    resp_body = {}

    if value:
        resp_header["status"] = STATUS_OK
        resp_body["value"] = value
    else:
        resp_header["status"] = STATUS_NOT_FOUND

    return utils.create_request(resp_header, resp_body)


def find_and_store_key(n, body):
    """
    Looks through chord for node where key should be stored and stores it there
    :param n: the node on which to call find_and_store_key
    :param body: the request body
    :return: string of response
    """
    resp_header = {
        "status": STATUS_OK
        if n.find_and_store_key(body["key"], body["value"])
        else STATUS_NOT_FOUND
    }

    return utils.create_request(resp_header, {})


def find_and_delete_key(n, body):
    """
    Looks through chord for node that has key stored and deletes it
    :param n: the node on which to call find_and_delete_key
    :param body: the request body
    :return: string of response
    """
    resp_header = {
        "status": STATUS_OK if n.find_and_delete_key(body["key"]) else STATUS_NOT_FOUND
    }

    return utils.create_request(resp_header, {})


def lookup(n, body):
    """
    Looks up a key in the data of this node
    :param n: the node whose data to look for the key in
    :param body: the request body
    :return: string of response
    """
    exists = body["key"] in n.storage
    resp_header = {"status": STATUS_OK if exists else STATUS_NOT_FOUND}
    resp_body = {}

    if exists:
        resp_body["value"] = n.storage[body["key"]]

    return utils.create_request(resp_header, resp_body)


# Functions that write to node object n
def update_predecessor(n, body):
    """
    Returns response to update_predecessor remote procedure call
    :param n: node on which to call update_predecessor
    :param body: body of request
    :return: string of response
    """

    # function to be run by main thread to update data
    def update(node):
        node.predecessor = Finger((body["ip"], body["port"]), body["node_id"])
        # if move fails, do nothing (keys are kept on this node)
        node.move_keys_to_predecessor()

    if not n.predecessor or utils.is_between_clockwise(
        body["node_id"], n.predecessor.node_id, n.node_id
    ):
        n.event_queue.put(update)

    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def clear_predecessor(n):
    """
    Sets n's predecessor to None
    :param n: node on which to call clear_predecessor
    :return: string of response
    """

    def clear(node):
        node.predecessor = None

    n.event_queue.put(clear)

    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def batch_store_keys(n, body):
    """
    Stores every (key, value) pair passed in the request
    :param n: the node into which to insert the pairs
    :param body: the request body
    :return: string of response
    """

    def store(node):
        for k_dict in body["keys"]:
            node.storage.add_key(k_dict["key"], k_dict["value"], k_dict["key_id"])

    n.event_queue.put(store)

    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def store_key(n, body):
    """
    Stores a new (key, value) pair to the data of this node
    :param n: the node into which to insert the pair
    :param body: the request body
    :return: string of response
    """

    def store(node):
        node.storage.add_key(body["key"], body["value"], body["key_id"])

    n.event_queue.put(store)

    resp_header = {"status": STATUS_OK}
    return utils.create_request(resp_header, {})


def delete_key(n, body):
    """
    Removes a key from the data of this node
    :param n: the node from which to remove the key
    :param body: the request body
    :return: string of response
    """

    def remove(node):
        del node.storage[body["key"]]

    n.event_queue.put(remove)

    resp_header = {
        "status": STATUS_OK if body["key"] in n.storage else STATUS_NOT_FOUND
    }
    return utils.create_request(resp_header, {})
