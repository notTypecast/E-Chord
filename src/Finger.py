from src import utils


class Finger:
	hash_func = None
	params = None

	def __init__(self, addr, node_id=None):
		self.addr = addr
		if not node_id:
			self.node_id = utils.get_id(addr[0] + str(addr[1]), Finger.hash_func, Finger.params)

	def __eq__(self, other):
		return self.addr == other.addr
