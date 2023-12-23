import socket
import selectors
import time
import json
from threading import Lock
#project files
from src import utils
from src.utils import log

class ConnectionPool:
    """
    Defines a connection pool for the node
    The pool handles all network communication for the node
    The pool contains all currently open connections, incomning and outgoing
    """
    def __init__(self, port):
        self.SERVER_ADDR = ("", port) if port is not None else (utils.get_ip(), utils.params["host"]["server_port"])
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server.bind(self.SERVER_ADDR)
        except socket.error:
            log.critical("Failed to bind server socket")
            exit(1)

        self.server.listen()
        self.server.setblocking(False)

        self.outgoing_connections = {}
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server, selectors.EVENT_READ, self._accept_connection_cb)

    def cleanup_outgoing(self):
        """
        Removes all outgoing connections that have timed out
        """
        self.outgoing_connections = {key: value for key, value in self.outgoing_connections.items() if time.time() - value[1] < utils.params["net"]["connection_lifespan"]}

    def send(self, peer_addr, request_msg, pre_request, hold_connection=True):
        """
        Sends a request to a peer with the given address
        Returns the response from the peer
        """
        client = self._get_connection(peer_addr, hold_connection)
        if not client:
            log.info(f"Failed to connect to {peer_addr}")
            return False
        
        enc_request_msg = request_msg.encode()
        if pre_request:
            pre_req_msg = utils.create_request({"type": "size"}, {"data_size": len(enc_request_msg)})
            # using $ as delimiter to identify pre-requests
            pre_req_msg = "$" + pre_req_msg + "$"
            try:
                client.sendall(pre_req_msg.encode())
            except (socket.error, socket.timeout):
                del self.outgoing_connections[peer_addr]
                self._get_connection(peer_addr, hold_connection)
                client.sendall(pre_req_msg.encode())

        try:
            client.sendall(enc_request_msg)
        except (socket.error, socket.timeout):
            del self.outgoing_connections[peer_addr]
            self._get_connection(peer_addr, hold_connection)
            client.sendall(enc_request_msg)

        data = client.recv(utils.params["net"]["data_size"]).decode()
        return data
        
    def _get_connection(self, addr, hold_connection):
        """
        Gets an (outgoing) connection to a peer
        If the connection is already open, it is returned and refreshed
        Otherwise, a new connection is opened
        """
        if addr in self.outgoing_connections:
            self.outgoing_connections[addr][1] = time.time()
            return self.outgoing_connections[addr][0]
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client.settimeout(utils.params["net"]["timeout"])
        try:
            client.connect(addr)
        except (socket.error, socket.timeout):
            return False
        
        if hold_connection:
            self.outgoing_connections[addr] = [client, time.time()]
        
        return client

    def select_incoming(self, handler):
        """
        Selects incoming connections
        """
        events = self.selector.select()
        for key, _ in events:
            key.data(key.fileobj, handler)
        
    def _accept_connection_cb(self, sock, handler=None):
        """
        Callback for accepting a new connection
        This is used to add connections to the pool that are initiated by other nodes
        """
        conn, addr = sock.accept()
        conn.setblocking(False)
        log.info(f"Accepted connection from {addr}")
        self.selector.register(conn, selectors.EVENT_READ, self._read_from_connection_cb)

    def _read_from_connection_cb(self, sock, handler):
        """
        Callback for reading from a connection
        """
        data = sock.recv(utils.params["net"]["data_size"])
        if data:
            handler(sock, data)
        else:
            log.info(f"Closing connection to {sock.getpeername()}")
            self.selector.unregister(sock)
            sock.close()

    def get_seed(self, node_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(utils.params["net"]["timeout"])
            for i in range(utils.params["seed_server"]["attempt_limit"]):
                try:
                    client.connect((utils.params["seed_server"]["ip"], utils.params["seed_server"]["port"]))
                    break
                except (socket.error, socket.timeout):
                    log.info(f"Failed to connect to seed server, retrying... "
                             f"{i + 1}/{utils.params['seed_server']['attempt_limit']}")
                    time.sleep(2)
            else:
                log.critical("Connection to seed failed (attempt limit reached)")
                exit(1)
            client.sendall(utils.create_request({"type": "get_seed"},
                                                {"ip": self.SERVER_ADDR[0], "port": self.SERVER_ADDR[1],
                                                 "node_id": node_id}).encode())
            data = json.loads(client.recv(utils.params["net"]["data_size"]).decode())

        return data
