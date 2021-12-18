import utils

class Finger:
	hash_func = None
	params = None

	def __init__(self, addr):
		self.addr = addr
		self.node_id = utils.get_id(addr[0] + str(addr[1]), Finger.hash_func, Finger.params)
