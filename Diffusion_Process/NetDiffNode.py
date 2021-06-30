import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Diffusion_Process.NetDiffGraphElement import NetDiffGraphElement
from Ontological.Constant import Constant

class NetDiffNode(NetDiffGraphElement):
	ID = "node"

	def __init__(self, id):
		super().__init__("node", [Constant(str(id))])
		self._color = "blue"

	def get_labels():
		return NetDiffNode._labels

	def __hash__(self):
		return super().__hash__()

	def to_json_string(self):
		#if random.randint(1, 50) == 1:
			#self._color = "red"
		return '{"id":"' + str(self._id) + '", "label": "' + str(self._id) + '", "color": "' + self._color + '" }'
		#return '{"id":"' + str(self._id) + '" }'

	def set_color(self, color):
		self._color = color

	def get_color(self):
		return self._color


	def __eq__(self, node):
		result = False
		if isinstance(self, type(node)):
			result = self is node
			result = result or (hash(self) == hash(node))

		return result