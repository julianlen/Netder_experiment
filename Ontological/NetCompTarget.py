import os, sys
import portion
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Ontological.Atom import Atom
from Ontological.Constant import Constant

class NetCompTarget(Atom):
	ID = "net_comp_target"

	def __init__(self, component, label, interval):
		terms = [Constant(str(hash(component))), Constant(label), Constant(interval.lower), Constant(interval.upper)]
		super().__init__(NetCompTarget.ID, terms)
		self._component = component
		self._label = label

	def getBound(self):
		return portion.closed(self._terms[2].getValue(), self._terms[3].getValue())

	def getLabel(self):
		#return self._terms[1].getValue()
		return self._label

	def getComponent(self):
		return self._component