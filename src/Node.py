import json
import socket
from hashlib import sha1
# project files
import utils
from RequestHandler import RequestHandler

hash_func = sha1
DATA_SIZE = 1024


class Node:
    """
    Defines an E-Chord Node
    """
    def __init__(self):
        """
        Initializes a new node
        """
        # get configuration settings from params.json
        with open("config/params.json") as f:
            self.params = json.load(f)

        self.SERVER_ADDR = (socket.gethostname(), self.params["host"]["server_port"])
        self.CLIENT_ADDR = (socket.gethostname(), self.params["host"]["client_port"])

        # ID will be SHA-1(IP+port)
        self.node_id = utils.get_id(self.SERVER_ADDR[0] + str(self.SERVER_ADDR[1]), hash_func, self.params)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.SERVER_ADDR)

        response = json.loads(self.get_seed())
        node_addr = (response["body"]["ip"], response["body"]["port"])
        request_msg = RequestHandler.create_request("get_successors", {"for_node": self.node_id})

        response = json.loads(Node.ask_peer(node_addr, request_msg))

        self.finger_table = json.loads(response["body"]["finger_table"])

        self.successor = self.finger_table[0]
        request_msg = RequestHandler.create_request("get_predecessor", {})
        self.predecessor = Node.ask_peer(self.successor, request_msg)

    @staticmethod
    def ask_peer(peer_addr, request_msg):
        """
        Makes request to peer, sending request_msg
        :param peer_addr: (IP, port) of peer
        :param request_msg: JSON string of request to send
        :return: string response of peer
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
            data = client.recv(DATA_SIZE).decode()

        return data

    def get_seed(self):
        """
        Gets an existing node from seed server
        :return: tuple of (IP, port)
        """
        self.server.connect((self.params["seed_server"]["ip"], self.params["seed_server"]["port"]))
        return self.server.recv(DATA_SIZE).decode()

    def listen(self):
        self.server.listen()
        while True:
            connection, address = self.server.accept()

            with connection:
                data = connection.recv(DATA_SIZE).decode()

                if not data:
                    break
