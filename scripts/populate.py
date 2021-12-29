import json
import socket
import time
from sys import argv

with open(argv[1]) as f:
    data = json.load(f)

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

for event in data:
    ask_peer(("", 9150), "find_and_store_key", {"key": event, "value": data[event]})

















