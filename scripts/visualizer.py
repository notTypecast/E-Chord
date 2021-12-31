import socket
import time
import json
from hashlib import sha1
import math
import os

os.chdir("../")


nodes = {}
nodes_p = {}
total_keys = {}

with open("config/params.json") as f:
    params = json.load(f)


def buildTable(data):
    leftTopCorner = "╔"
    leftBottomCorner = "╚"
    rightTopCorner = "╗"
    rightBottomCorner = "╝"
    hSide = "═"
    hSideBottom = "╦"
    hSideTop = "╩"
    vSide = "║"
    vSideRight = "╠"
    vSideLeft = "╣"
    center = "╬"

    columnMaximums = []

    column = 0
    while column < len(data[0]):
        row = 0
        columnMax = 0
        while row < len(data):
            if len(data[row][column]) > columnMax:
                columnMax = len(data[row][column])

            row += 1

        columnMaximums.append(columnMax)

        column += 1

    vSideIndexes = []
    for maximum in columnMaximums:
        vSideIndexes.append(maximum + 3)

    topLine = leftTopCorner
    bottomLine = leftBottomCorner
    middleLine = vSideRight

    for index in vSideIndexes:
        indexSide = hSide * (index - 1)

        topLine += indexSide + hSideBottom
        bottomLine += indexSide + hSideTop
        middleLine += indexSide + center

    topLine = topLine[:-1] + rightTopCorner
    bottomLine = bottomLine[:-1] + rightBottomCorner
    middleLine = middleLine[:-1] + vSideLeft

    dataLines = []

    for row in data:
        currItem = 0
        rowString = ""
        while currItem < len(row):
            padSpace = " " * ((vSideIndexes[currItem] - len(row[currItem])) // 2)
            toAdd = vSide + padSpace + row[currItem]
            rowString += toAdd + " " * (vSideIndexes[currItem] - len(toAdd))

            currItem += 1

        dataLines.append(rowString + vSide)

    helpMessage = topLine + "\n"

    currDataLine = 0
    while currDataLine < len(dataLines):
        helpMessage += dataLines[currDataLine] + "\n"
        if currDataLine != len(dataLines) - 1:
            helpMessage += middleLine + "\n"

        currDataLine += 1

    helpMessage += bottomLine

    return helpMessage


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
    allkeys = 0
    for port in ports:
        ID = get_id(str(port), sha1)

        requests = ["get_successor", "get_predecessor", "debug_get_total_keys"]
        dicts = [nodes, nodes_p, total_keys]

        for i in range(3):
            req = requests[i]
            d = dicts[i]
            r = ask_peer(("", port), req, {})
            if r:
                if r["header"]["status"] in range(200, 300):
                    if i != 2:
                        d[port, ID] = r["body"]["node_id"]
                    else:
                        d[port, ID] = r["body"]["total_keys"]
                        allkeys += d[port, ID]
                else:
                    d[port, ID] = "None"
            else:
                # print(f"Couldn't reach node {ID}")
                try:
                    # this might cause an actual node to be deleted due to conflict
                    del d[port, ID]
                except KeyError:
                    pass

    os.system("clear")

    ALL = [["Successors", "Predecessors", "Keys"]]

    l = sorted(list(nodes), key=lambda key: key[1])[::-1]
    for i in range(len(l)):
        try:
            ALL.append([
                f"{l[len(l) - i - 1][1]} -> {nodes[l[len(l) - i - 1]]}",
                f"{l[i][1]} <- {nodes_p[l[i]]}",
                f"{l[len(l) - i - 1][1]}: {total_keys[l[len(l) - i - 1]]}"
            ])
        except KeyError:
            pass

    table = buildTable(ALL)
    print(table)
    print(f"Total keys: {allkeys}")

    time.sleep(3)
