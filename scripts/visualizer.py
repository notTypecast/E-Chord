import socket
import time
import json
from hashlib import sha1
import math
import os

os.chdir("../")


nodes = {}
nodes_p = {}

with open("config/params.json") as f:
    params = json.load(f)


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

    return res_id % 2 ** ring_size


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
    Makes request to peer, sending request_msg
    :param peer_addr: (IP, port) of peer
    :param req_type: type of request for request header
    :param body_dict: dictionary of body
    :param return_json: determines if json or string response should be returned
    :return: string response of peer
    """

    request_msg = create_request({"type": req_type}, body_dict)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(5)
        try:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
            data = client.recv(1024).decode()
        except (socket.error, socket.timeout):
            return None

    if not data:
        return None

    return data if not return_json else json.loads(data)


ports = range(params["testing"]["initial_port"], params["testing"]["initial_port"] + params["testing"]["total_nodes"])
while True:

    for port in ports:
        ID = get_id(str(port), sha1)
        req = "get_successor"
        d = nodes

        for _ in range(2):
            r = ask_peer(("", port), req, {})
            if r:
                if r["header"]["status"] in range(200, 300):
                    d[ID] = r["body"]["node_id"]
                else:
                    d[ID] = "None"
            else:
                # print(f"Couldn't reach node {ID}")
                try:
                    # this might cause an actual node to be deleted due to conflict
                    del d[ID]
                except:
                    pass

            req = "get_predecessor"
            d = nodes_p

    os.system("clear")

    l = sorted(list(nodes))[::-1]
    print("Successors\t\tPredecessors")
    for i in range(len(l)):
        print(l[len(l) - i - 1], "->", nodes[l[len(l) - i - 1]], "\t\t", l[i], "<-", nodes_p[l[i]])

    time.sleep(3)
