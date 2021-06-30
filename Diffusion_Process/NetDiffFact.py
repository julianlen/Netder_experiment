import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Ontological.NetCompTarget import NetCompTarget
from Ontological.Constant import Constant

class NetDiffFact(NetCompTarget):
	ID = "net_diff_fact"

	def __init__(self, component, label, interval, t_lower, t_upper):
		super().__init__(component, label, interval)
		self._terms.append(Constant(t_lower))
		self._terms.append(Constant(t_upper))
		self._id = NetDiffFact.ID

	def getTimeLower(self):
		return self._terms[4].getValue()

	def getTimeUpper(self):
		return self._terms[5].getValue()