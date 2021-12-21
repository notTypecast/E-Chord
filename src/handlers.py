import utils
from RequestHandler import RequestHandler
from Finger import Finger

"""
handlers.py
| Contains functions which handle remote procedure calls
| Each request type is mapped to its corresponding function in REQUEST_MAP
| The request handler thread calls the corresponding function using the request type found in the header
| For functions that require writing to the node object on which the remote procedure is called, a function which 
accepts the node object as an argument and writes the changes is added to the queue; the main thread will then
remove the callable object from the queue and call it, writing the changes
"""

STATUS_OK = 200
STATUS_NOT_FOUND = 404

# Map RPC types with handlers
REQUEST_MAP = {
    "get_successor": lambda n, event_queue, body: get_successor(n),
    "get_predecessor": lambda n, event_queue, body: get_predecessor(n),
    "find_successor": lambda n, event_queue, body: find_successor(n, body),
    "get_closest_preceding_finger": lambda n, event_queue, body: get_closest_preceding_finger(n, body),
    "update_predecessor": lambda n, event_queue, body: update_predecessor(event_queue, body),
}


# Functions that only read from node object n
def get_successor(n):
    """
    Returns response to get_successor remote procedure call
    :param n: node whose successor to return
    :return: string of response
    """
    header = {"status": STATUS_OK, "type": "successor"}
    body = {"ip": n.finger_table[0].addr[0], "port": n.finger_table[0].addr[1], "node_id": n.finger_table[0].node_id}

    return RequestHandler.create_request(header, body)


def get_predecessor(n):
    """
    Returns response to get_predecessor remote procedure call
    :param n: node whose predecessor to return
    :return: string of response
    """
    header = {"type": "predecessor"}
    body = {}
    if n.predecessor:
        header["status"] = STATUS_OK
        body = {"ip": n.predecessor.addr[0], "port": n.predecessor.addr[1], "node_id": n.predecessor.node_id}
    else:
        header["status"] = STATUS_NOT_FOUND

    return RequestHandler.create_request(header, body)


def find_successor(n, body):
    """
    Returns response to find_successor remote procedure call
    :param n: node on which to call find_successor method
    :param body: body of request
    :return: string of response
    """
    successor_data = n.find_successor(body["for_id"])
    header = {"status": STATUS_OK, "type": "successor"}
    body = {"ip": successor_data[0], "port": successor_data[1], "node_id": successor_data[2]}

    return RequestHandler.create_request(header, body)


def get_closest_preceding_finger(n, body):
    """
    Returns response to get_closest_preceding_finger remote procedure call
    :param n: node on which to call closest_preceding_finger
    :param body: body of request
    :return: string of response
    """
    finger = n.closest_preceding_finger(body["for_key_id"])

    header = {"status": STATUS_OK, "type": "closest_preceding_finger"}
    body = {"ip": finger.addr[0], "port": finger.addr[1], "node_id": finger.node_id}

    return RequestHandler.create_request(header, body)


def get_prev_successor_list(n):
    """
    Returns successor list for previous node
    Successor list will contain n's successor as first entry, and all nodes in n's successor list up to r-1
    :param n:
    :return:
    """
    header = {"status": STATUS_OK, "type": "prev_successor_list"}
    prev_successor_list = [{"ip": n.finger_table[0].addr[0], "port": n.finger_table[0].addr[1],
                            "node_id": n.finger_table[0].node_id}]
    for succ in n.successor_list[:-1]:
        prev_successor_list.append({"ip": succ.addr[0], "port": succ.addr[1], "node_id": succ.node_id})
    body = {"successor_list": prev_successor_list}

    return RequestHandler.create_request(header, body)


# Functions that write to node object n
def update_predecessor(n, event_queue, body):
    """
    Returns response to update_predecessor remote procedure call
    :param n: node on which to call update_predecessor
    :param event_queue: queue shared between threads
    :param body: body of request
    :return: string of response
    """
    # function to be run by main thread to update data
    def update(node):
        node.predecessor = Finger((body["ip"], body["port"]), body["node_id"])
    if not n.predecessor or utils.is_between_clockwise(body["node_id"], n.predecessor.node_id, n.node_id):
        event_queue.put(update)

    header = {"status": STATUS_OK}
    body = {}
    return RequestHandler.create_request(header, body)
