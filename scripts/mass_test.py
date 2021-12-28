import multiprocessing
import os
from time import sleep
import json
import socket
from random import choice

os.chdir("../")

with open("config/params.json") as f:
    params = json.load(f)


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


def ask_peer(peer_addr, req_type, body_dict, return_json=True):
    """
    Edited version of ask_peer for general use outside Node
    Sends a request and returns the response
    :param peer_addr: (IP, port) of peer
    :param req_type: type of request for request header
    :param body_dict: dictionary of body
    :param return_json: determines if json or string response should be returned
    :return: string response of peer
    """

    request_msg = create_request({"type": req_type}, body_dict)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
        except (socket.error, socket.timeout):
            pass


def start_node(porty):
    os.system(f"python3 start_node.py {porty}")


port = params["testing"]["initial_port"]

processes = []
node_details = []

for _ in range(params["testing"]["total_nodes"]):
    new_node = multiprocessing.Process(target=start_node, args=(port,))
    new_node.daemon = True
    processes.append(new_node)
    node_details.append(port)
    new_node.start()
    port += 1
    sleep(1)

sleep(params["testing"]["stabilize_wait_period"])

total_nodes = list(node_details)
total_removed = 0

while total_removed * 100 / len(node_details) < params["testing"]["percentage_to_remove"]:
    n = choice(total_nodes)
    ask_peer(("", n), "leave_ring", {})
    total_nodes.remove(n)
    total_removed += 1
    print(f"total_removed: {total_removed}")

try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    for process in processes:
        process.terminate()
