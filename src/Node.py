import json
import socket
import time
from hashlib import sha1
from random import randint
import threading
from queue import Queue
# project files
import utils
from RequestHandler import RequestHandler
from Finger import Finger
from handlers import REQUEST_MAP

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

        # initialize
        self.finger_table = [Finger(self.SERVER_ADDR)]*self.params["ring"]["bits"]

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # ID will be SHA-1(IP+port)
        self.node_id = utils.get_id(self.SERVER_ADDR[0] + str(self.SERVER_ADDR[1]), hash_func, self.params)

        # get initial node from seed server
        seed_addr = self.get_seed()

        # Join ring
        # if at least one other node exists
        if seed_addr:
            self.predecessor = None
            response = Node.ask_peer(seed_addr, "find_successor", {"for_id", self.node_id})
            self.finger_table[0] = Finger((response["body"]["ip"], response["body"]["ip"]))

        # if this is the first node
        else:
            self.predecessor = Finger(self.SERVER_ADDR)

        self.listen()

    def stabilize(self):
        """
        Stabilize ring by updating successor or successor's predecessor
        :return: None
        """
        response = Node.ask_peer(self.finger_table[0].addr, "get_predecessor", {})
        status_ok = response["header"]["status"] in range(200, 300)

        if status_ok and self.node_id < response["body"]["node_id"] < self.finger_table[0].node_id:
            self.finger_table[0] = Finger((response["body"]["ip"], response["body"]["port"]))
        else:
            Node.ask_peer(self.finger_table[0].addr, "update_predecessor", {"ip": self.SERVER_ADDR[0],
                                                                            "port": self.SERVER_ADDR[1],
                                                                            "node_id": self.node_id})

    def fix_fingers(self):
        """
        Fixes a random finger of the finger table
        :return: None
        """
        i = randint(1, self.params["ring"]["bits"])
        succ = self.find_successor(self.node_id + 2**i)
        self.finger_table[i] = Finger((succ[0], succ[1]), succ[2])

    def find_successor(self, key_id):
        """
        Finds successor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        current_node = self.find_predecessor(key_id)
        response = Node.ask_peer(current_node, "get_successor", {})
        return response["body"]["ip"], response["body"]["port"], response["body"]["node_id"]

    def find_predecessor(self, key_id):
        """
        Finds predecessor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        current_node = self
        successor_id = self.finger_table[0].node_id

        while current_node.node_id < key_id <= successor_id:
            if current_node.addr == self.SERVER_ADDR:
                current_node = self.closest_preceding_finger(key_id)
            else:
                # TODO maybe check for status
                response = Node.ask_peer(current_node.addr, "get_closest_preceding_finger", {"for_key_id": key_id})
                current_node = Finger((response["body"]["ip"], response["body"]["port"]), response["body"]["node_id"])
                response = Node.ask_peer((current_node.addr[0], current_node.addr[1]), "get_successor", {})
                successor_id = response["body"]["node_id"]

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
        return Finger(self.SERVER_ADDR, self.node_id)

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
            client.sendall(RequestHandler.create_request("add_node",
                                                         {"ip": self.SERVER_ADDR[0], "port": self.SERVER_ADDR[1]}))
            data = json.loads(client.recv(DATA_SIZE).decode())["body"]

        return data["ip"], data["port"]

    def listen(self):
        """
        Main server loop
        Runs node and listens for requests from other nodes, then creates a new thread to handle them
        :return: None
        """
        # bind server to IP
        self.server.bind(self.SERVER_ADDR)
        self.server.listen()

        # create threads to listen for connections and to send stabilize signal
        event_queue = Queue()

        # accept incoming connections
        connection_acceptor = threading.Thread(target=self.accept_connections, args=(self.server, event_queue))
        connection_acceptor.daemon = True

        # initialize timer for stabilization of node
        stabilizer = threading.Thread(target=self.stabilize_timer,
                                      args=(event_queue, self.params["ring"]["stabilize_delay"]))
        stabilizer.daemon = True

        connection_acceptor.start()
        stabilizer.start()

        while True:
            # wait until event_queue is not empty, then pop
            data = event_queue.get()

            # 0 is used for stabilize event
            if not data:
                self.stabilize()
                self.fix_fingers()
                continue

            # call function of the event_queue to update state in main thread
            if callable(data):
                data(self)
                continue

            # for every connection, start new thread to handle it
            connection_handler = threading.Thread(target=self.handle_connection, args=(self, event_queue, data))
            connection_handler.start()

    # Thread Methods
    @staticmethod
    def accept_connections(server, event_queue):
        """
        Accepts a new connection on the passed socket and places it in queue
        :param server: the socket
        :param event_queue: shared queue
        :return: None
        """
        while True:
            # accept connection and add to event queue for handling
            # main thread will start a new thread to handle it
            conn_details = server.accept()
            event_queue.put(conn_details)

    @staticmethod
    def handle_connection(node, event_queue, conn_details):
        """
        Handles existing connection until it closes
        :param conn_details: connection details (connection, address)
        :return: None
        """
        connection, address = conn_details

        with connection:
            data = connection.recv(DATA_SIZE).decode()

            if not data:
                return

            data = json.loads(data)

            # select RPC handler according to RPC type
            REQUEST_MAP[data["type"]](node, event_queue, data["body"])

    @staticmethod
    def stabilize_timer(event_queue, delay):
        """
        Sleeps for specified amount of time, then places 0 in queue
        :param event_queue: shared queue
        :param delay: amount of time to sleep for
        :return: None
        """
        while True:
            time.sleep(delay)
            event_queue.put(0)
