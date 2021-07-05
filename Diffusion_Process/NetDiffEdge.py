import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Diffusion_Process.NetDiffGraphElement import NetDiffGraphElement
from Ontological.Constant import Constant

class NetDiffEdge(NetDiffGraphElement):
	ID = "edge"

	def __init__(self, source, target):
		super().__init__("edge", [Constant(str(source)), Constant(str(target))])

	
	def get_labels():
		return NetDiffEdge._labels

	def __hash__(self):
		return super().__hash__()
	
	def to_json_string(self):
		return '{"id":"'+ str(self._id) +'", "from":'+ str(self._terms[0].getValue()) + ', "to":' + str(self._terms[1].getValue()) + ', "color": "black" ' + '}'
		
	def getSource(self):
		return self._terms[0].getValue()

	def getTarget(self):
		return self._terms[0].getValue()

	def __eq__(self, edge):
		result = False
		if isinstance(self, type(edge)):
			result = self is edge

			result = result or (hash(self) == hash(edge))

		return result
