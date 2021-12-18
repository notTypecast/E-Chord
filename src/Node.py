import json
from copy import copy
import socket
from hashlib import sha1
# project files
import utils
from RequestHandler import RequestHandler
from Finger import Finger

hash_func = sha1
Finger.hash_func = hash_func
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
            Finger.params = self.params

        # set address for server and client
        self.SERVER_ADDR = (socket.gethostname(), self.params["host"]["server_port"])
        self.CLIENT_ADDR = (socket.gethostname(), self.params["host"]["client_port"])

        # ID will be SHA-1(IP+port)
        self.node_id = utils.get_id(self.SERVER_ADDR[0] + str(self.SERVER_ADDR[1]), hash_func, self.params)

        # bind server to IP
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.SERVER_ADDR)

        # get initial node from seed server
        seed_addr = self.get_seed()

        self.join_ring(seed_addr)



    def join_ring(self, seed_addr):
        self.finger_table = []
        # at least one other node exists
        if seed_addr:
            self.init_finger_table(seed_addr)



        # this is the first node
        else:
            # all fingers and predecessor point to self, the only node in the network
            for i in range(0, self.params["ring"]["bits"]):
                self.finger_table.append(Finger(self.SERVER_ADDR))
            self.predecessor = self.SERVER_ADDR

    def init_finger_table(self, seed_addr):
        # NOTE: self.node_id successor is predecessor's successor, which is not this node (since it hasn't joined yet)
        response = Node.ask_peer(seed_addr, "find_successor", {"for_id": self.node_id})
        self.finger_table.append(Finger((response["body"]["ip"], response["body"]["port"])))

        # update own predecessor
        response = Node.ask_peer(self.finger_table[0].addr, "get_predecessor", {})
        self.predecessor = response["body"]["ip"], response["body"]["port"]

        # update successor's predecessor to be this node
        # TODO status??
        Node.ask_peer(self.finger_table[0].addr, "update_predecessor", {"ip": self.SERVER_ADDR[0], "port": self.SERVER_ADDR[1]})

        for i in range(1, self.params["ring"]["bits"]):
            new_finger_start = self.node_id + 2**(i + 1)
            if self.node_id <= new_finger_start < self.finger_table[i].node_id:
                new_finger = copy(self.finger_table[i])
                self.finger_table.append(new_finger)
            else:
                response = Node.ask_peer(seed_addr, "find_successor", {"for_id": new_finger_start})
                new_finger = Finger((response["body"]["ip"], response["body"]["port"]))
                self.finger_table.append(new_finger)

    def find_successor(self, key_id):
        """
        Finds successor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        current_node = self.find_predecessor(key_id)
        response = Node.ask_peer(current_node, "get_successor", {})
        return response["body"]["ip"], response["body"]["port"]

    def find_predecessor(self, key_id):
        """
        Finds predecessor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        current_node = self

        while self.node_id < key_id <= utils.get_id(self.finger_table[0].addr, hash_func, self.params):
            if current_node.addr == self.SERVER_ADDR:
                current_node = self.closest_preceding_finger(key_id)
            else:
                # TODO maybe check for status
                response = Node.ask_peer(current_node.addr, "get_closest_preceding_finger", {"for_key_id": key_id})
                node_addr = response["body"]["ip"], response["body"]["port"]
                current_node = Finger(node_addr)

        return current_node.addr

    def closest_preceding_finger(self, key_id):
        """
        Gets closest preceding finger for id key_id
        :param key_id: the ID
        :return: Finger pointing to the closest preceding Node, or self if self is the closest preceding Node
        """
        for i in range(0, self.params["ring"]["bits"], -1):
            if self.node_id < self.finger_table[i].node_id < key_id:
                return self.finger_table[i]
        return Finger(self.SERVER_ADDR)


    @staticmethod
    def ask_peer(peer_addr, req_type, body_dict, return_json=True):
        """
        Makes request to peer, sending request_msg
        :param peer_addr: (IP, port) of peer
        :param req_type: type of request for request header
        :param body_dict: dictionary of body
        :param return_json: determines if json or string response should be returned
        :return: string response of peer
        """
        request_msg = RequestHandler.create_request({"type": req_type}, body_dict)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(peer_addr)
            client.sendall(request_msg.encode())
            data = client.recv(DATA_SIZE).decode()

        return data if not return_json else json.loads(data)

    def get_seed(self):
        """
        Gets an existing node from seed server
        :return: tuple of (IP, port)
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.params["seed_server"]["ip"], self.params["seed_server"]["port"]))
            client.sendall(RequestHandler.create_request("add_node", {"ip": self.SERVER_ADDR[0], "port": self.SERVER_ADDR[1]}))
            data = json.loads(client.recv(DATA_SIZE).decode())["body"]

        return data["ip"], data["port"]

    def listen(self):
        self.server.listen()
        while True:
            connection, address = self.server.accept()

            with connection:
                data = connection.recv(DATA_SIZE).decode()

                if not data:
                    break
