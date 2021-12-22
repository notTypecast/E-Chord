import json
import socket
import time
from hashlib import sha1
import random
import threading
from queue import Queue
from copy import copy
# project files
from src import utils
from src.utils import log
from src.Finger import Finger
from src.handlers import REQUEST_MAP

hash_func = sha1
Finger.hash_func = hash_func


class Node:
    """
    Defines an E-Chord Node
    """

    params = None

    def __init__(self, port):
        """
        Initializes a new node
        """
        # get configuration settings from params.json
        with open("config/params.json") as f:
            Node.params = json.load(f)
            Finger.params = Node.params

        log.info("Loaded params")

        # set address for server and client
        self.SERVER_ADDR = ("", port)

        # initialize finger table and successor list
        self.finger_table = [Finger(self.SERVER_ADDR)] * Node.params["ring"]["bits"]
        self.successor_list = [None] * Node.params["ring"]["successor_list_length"]

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # ID will be SHA-1(IP+port)
        self.node_id = utils.get_id(self.SERVER_ADDR[0] + str(self.SERVER_ADDR[1]), hash_func, Node.params)
        log.debug(f"Initialized with node ID: {self.node_id}")

        while True:
            # get initial node from seed server
            data = self.get_seed()

            # Join ring
            # if at least one other node exists
            if data["header"]["status"] in range(200, 300):
                log.info("Got seed from seed server")
                log.debug(f"Seed address: {data['body']['ip'], data['body']['port']}")
                seed_dead = False
                while True:
                    self.predecessor = None
                    # get successor from seed node
                    log.info("Asking seed for successor")
                    response = Node.ask_peer((data["body"]["ip"], data["body"]["port"]),
                                             "find_successor", {"for_id": self.node_id})
                    if not response:
                        # tell seed server that seed node has died
                        Node.ask_peer((Node.params["seed_server"]["ip"], Node.params["seed_server"]["port"]),
                                      "dead_node", {"ip": data["body"]["ip"], "port": data["body"]["port"]})
                        seed_dead = True
                        break

                    self.finger_table[0] = Finger((response["body"]["ip"], response["body"]["ip"]),
                                                  response["body"]["node_id"])
                    log.info("Got successor")
                    log.debug(f"Successor address: {self.finger_table[0].addr} with node ID: "
                              f"{self.finger_table[0].node_id}")

                    # initialize successor list, or get new successor if successor is dead
                    if self.init_successor_list():
                        log.info("Successfully initialized successor list")
                        break

                    # if successor has died, wait for other nodes to stabilize before asking for new successor
                    log.info("Waiting for stabilization")
                    time.sleep(Node.params["ring"]["stabilize_delay"])

                # if seed node is dead, reseed
                if seed_dead:
                    log.info("Seed is dead, retrying...")
                    continue

            # if this is the first node
            else:
                log.info("No other nodes in the network")
                self.predecessor = Finger(self.SERVER_ADDR)
                for i in range(len(self.successor_list)):
                    self.successor_list[i] = copy(self.predecessor)
                log.info("Initialized predecessor and sucessor list to self")

            break

        self.listen()

    def init_successor_list(self):
        """
        Initializes successor_list
        :return: True if successful, False if this node's successor is dead
        """
        # ask successor for this node's successor list
        log.info("Asking successor for this node's successor list")
        response = Node.ask_peer(self.finger_table[0].addr, "get_prev_successor_list", {})
        # successor is dead
        if not response:
            log.info("Successor is dead")
            return False

        # populating this node's successor list
        for i, successor in enumerate(response["body"]["successor_list"]):
            self.successor_list[i] = Finger((successor["ip"], successor["port"]), successor["node_id"])
        log.info("Successor list initialized successfully")

    def stabilize(self):
        """
        Stabilize ring by updating successor or successor's predecessor
        :return: None
        """
        log.info("Stabilizing...")
        current_successor = self.finger_table[0]

        # ask all successors in successor list until one responds, remove dead ones
        while len(self.successor_list):
            response = Node.ask_peer(current_successor.addr, "get_predecessor", {})
            if not response:
                # current successor was dead, get a new one from the successor list
                log.info("Successor is dead, getting next in list")
                log.debug(f"Successor dead is: {current_successor.addr}")
                current_successor = self.successor_list[0]
                del self.successor_list[0]
                continue
            self.finger_table[0] = current_successor
            break
        else:
            log.critical("All successors in successor list are dead")
            exit(1)

        status_ok = response["header"]["status"] in range(200, 300)

        if status_ok:
            # if successor has this node as predecessor
            if self.node_id == response["body"]["node_id"]:
                log.debug("Successor's predecessor is this node")
                return
            # if new node joined between this node and its successor
            if utils.is_between_clockwise(response["body"]["node_id"], self.node_id, self.finger_table[0].node_id):
                # shift successor list by 1
                self.successor_list.insert(0, self.finger_table[0])
                del self.successor_list[-1]
                # update successor
                self.finger_table[0] = Finger((response["body"]["ip"], response["body"]["port"]),
                                              response["body"]["node_id"])
                log.info("Got new successor")
                log.debug(f"New succesor address: {response['body']['ip'], response['body']['port']} with node ID: "
                          f"{response['body']['node_id']}")

            # update successor's predecessor to be this node
            Node.ask_peer(self.finger_table[0].addr, "update_predecessor", {"ip": self.SERVER_ADDR[0],
                                                                            "port": self.SERVER_ADDR[1],
                                                                            "node_id": self.node_id})
            log.debug("Asked successor to make this node its predecessor")

    def fix_fingers(self):
        """
        Fixes a random finger of the finger table
        :return: None
        """
        # TODO maybe priority here (and in successor list?)
        log.info("Fixing a finger...")
        i = random.randint(1, Node.params["ring"]["bits"])
        log.debug(f"Picked finger {i}")
        succ = self.find_successor(self.node_id + 2 ** i)
        self.finger_table[i] = Finger((succ[0], succ[1]), succ[2])

    def fix_successor_list(self):
        """
        | Picks a random successor from successor_list[:-1] and asks for its successor
        | If successor is the same as the next successor in the list, does nothing
        | If it is different, looks through list and tries to find returned node
        | If it is found, removes all nodes between it and the one that returned it
        | If it is not found, adds new successor to its correct index and removes everything between it and the one that
        returned it
        :return: None
        """
        log.info("Fixing a node in successor list...")
        # pick random node
        node_index = random.randint(0, len(self.successor_list) - 1)
        log.debug(f"Picked successor {node_index}")
        new_node_id = None
        last_alive_node_index = None
        last_alive_node_id = None

        # hold the dead nodes to be removed
        dead_nodes = set()

        # look at that node and each node before it
        while node_index != -1:
            # ping successor and get its successor if alive
            response = Node.ask_peer(self.successor_list[node_index].addr, "get_successor", {})
            if not response:
                log.info("Found dead successor in list")
                log.debug(f"ID found dead: {self.successor_list[node_index].node_id}")
                dead_nodes.add(self.successor_list[node_index])
                node_index -= 1
                continue
            # if last node was picked and is alive, return
            elif node_index == len(self.successor_list) - 1:
                log.debug("Successor picked randomly was last in list")
                # if successor list is at max capacity, do nothing
                if len(self.successor_list) == Node.params["ring"]["successor_list_length"]:
                    log.debug("Successor list is at max capacity")
                    return
                # else, append successor returned to successor list
                self.successor_list.append(Finger((response["body"]["ip"], response["body"]["port"]),
                                                  response["body"]["node_id"]))
                return

            # if it is alive, save its id
            new_node_id = response["body"]["node_id"]
            last_alive_node_id = self.successor_list[node_index].node_id
            last_alive_node_index = node_index
            break

        # if new_node_id is None, ask successor for its successor
        if new_node_id is None:
            response = Node.ask_peer(self.finger_table[0].addr, "get_successor", {})
            # if successor is dead, stop and wait for stabilize to fix it
            if not response:
                log.info("Successor is dead, waiting for next stabilize")
                for node in dead_nodes:
                    self.successor_list.remove(node)
                return
            new_node_id = response["body"]["node_id"]
            last_alive_node_id = self.finger_table[0].node_id
            last_alive_node_index = 0

        log.debug(f"Alive node ID: {last_alive_node_id}")

        # find position in successor list where new successor should be placed
        for i, succ in enumerate(self.successor_list[last_alive_node_index:]):
            if succ.node_id == new_node_id:
                break

            if utils.is_between_clockwise(new_node_id, last_alive_node_id, succ.node_id):
                self.successor_list.insert(i, Finger((response["body"]["ip"], response["body"]["port"]),
                                                     new_node_id))
                break

            dead_nodes.add(succ)

        for node in dead_nodes:
            self.successor_list.remove(node)

        self.successor_list = self.successor_list[:Node.params["ring"]["successor_list_length"]]
        log.info("Updated successor list")

    def find_successor(self, key_id):
        """
        Finds successor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        log.debug(f"Finding successor for ID: {key_id}")
        # if this is the only node in the network
        if self.finger_table[0].node_id == self.node_id:
            return self.SERVER_ADDR[0], self.SERVER_ADDR[1], self.node_id

        current_node = self.find_predecessor(key_id)
        response = Node.ask_peer(current_node, "get_successor", {})
        return response["body"]["ip"], response["body"]["port"], response["body"]["node_id"]

    def find_predecessor(self, key_id):
        """
        Finds predecessor for key_id
        :param key_id: the ID
        :return: tuple of (ID, port)
        """
        log.debug(f"Finding predecessor for ID: {key_id}")
        current_node = self
        successor_id = self.finger_table[0].node_id

        # while key_id is not between node_id and successor_id (while moving clockwise)
        while not utils.is_between_clockwise(key_id, current_node.node_id, successor_id, inclusive_upper=True):
            # TODO maybe check for status
            # TODO deal with failed responses here
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
        # TODO request finger found before returning to ensure its alive, don't return it if it isn't
        log.debug(f"Finding closest preceding finger for ID: {key_id}")
        for i in range(Node.params["ring"]["bits"], 0, -1):
            if utils.is_between_clockwise(self.finger_table[i].node_id, self.node_id, key_id):
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
        request_msg = utils.create_request({"type": req_type}, body_dict)
        log.debug("Asking peer")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client.settimeout(Node.params["net"]["timeout"])
            try:
                client.connect(peer_addr)
                client.sendall(request_msg.encode())
                data = client.recv(Node.params["net"]["data_size"]).decode()
            except (socket.error, socket.timeout) as err:
                log.error(err)
                return None

        return data if not return_json else json.loads(data)

    def get_seed(self):
        """
        Gets an existing node from seed server
        :return: tuple of (IP, port)
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(Node.params["net"]["timeout"])
            for i in range(Node.params["seed_server"]["attempt_limit"]):
                try:
                    client.connect((Node.params["seed_server"]["ip"], Node.params["seed_server"]["port"]))
                    break
                except (socket.error, socket.timeout) as err:
                    log.info(f"Failed to connect to seed server, retrying... "
                             f"{i + 1}/{Node.params['seed_server']['attempt_limit']}")
            else:
                log.critical("Connection to seed failed (attempt limit reached)")
                exit(1)
            client.sendall(utils.create_request({"type": "add_node"}, {"ip": self.SERVER_ADDR[0],
                                                                       "port": self.SERVER_ADDR[1]}).encode())
            try:
                data = json.loads(client.recv(Node.params["net"]["data_size"]).decode())
            except socket.timeout:
                pass

        return data

    def listen(self):
        """
        Main server loop
        Runs node and listens for requests from other nodes, then creates a new thread to handle them
        :return: None
        """
        log.info(f"Starting node on {self.SERVER_ADDR[0]}:{self.SERVER_ADDR[1]}")
        # bind server to IP
        self.server.bind(self.SERVER_ADDR)
        self.server.listen()

        # create threads to listen for connections and to send stabilize signal
        event_queue = Queue()

        # accept incoming connections
        connection_acceptor = threading.Thread(target=self.accept_connections, args=(self.server, self, event_queue))
        connection_acceptor.name = "Connection Acceptor"
        connection_acceptor.daemon = True

        # initialize timer for stabilization of node
        stabilizer = threading.Thread(target=self.stabilize_timer,
                                      args=(event_queue, Node.params["ring"]["stabilize_delay"]))
        stabilizer.name = "Stabilizer"
        stabilizer.daemon = True

        connection_acceptor.start()
        stabilizer.start()

        while True:
            # wait until event_queue is not empty, then pop
            data = event_queue.get()

            log.debug(f"Popped {data} from event queue")

            # data == 0 is used for stabilize event
            if not data:
                self.stabilize()
                self.fix_successor_list()
                self.fix_fingers()
                continue

            # if data is function, call it to update state in main thread
            if callable(data):
                data(self)

    # Thread Methods
    @staticmethod
    def accept_connections(server, node, event_queue):
        """
        Accepts a new connection on the passed socket and places it in queue
        :param server: the socket
        :param node: the node
        :param event_queue: shared queue
        :return: None
        """
        while True:
            # accept connection and add to event queue for handling
            # main thread will start a new thread to handle it
            conn_details = server.accept()
            log.info(f"Got new connection from: {conn_details[1]}")

            # data is connection, so start new thread to handle it
            connection_handler = threading.Thread(target=Node.handle_connection, args=(node, event_queue, conn_details))
            connection_handler.name = f"{conn_details[1]} Handler"
            connection_handler.start()

    @staticmethod
    def handle_connection(node, event_queue, conn_details):
        """
        Handles existing connection until it closes
        :param conn_details: connection details (connection, address)
        :param node: the node on which to call the method
        :param event_queue: shared queue
        :return: None
        """
        connection, address = conn_details

        with connection:
            data = connection.recv(Node.params["net"]["data_size"]).decode()
            if not data:
                return

            data = json.loads(data)

            # select RPC handler according to RPC type
            log.info(f"Got RPC call of type: {data['header']['type']}")
            response = REQUEST_MAP[data["header"]["type"]](node, event_queue, data["body"])

            connection.sendall(response.encode())

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
