import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from abc import ABC
from Ontological.Atom import Atom
from Diffusion_Process.NetDiffWorld import NetDiffWorld

class NetDiffGraphElement(Atom):
	_labels = []

	def __init__(self, id, terms):
		super().__init__(id, terms)


	def setLabels(self, labels):
		type(self)._labels = labels

	def getLabels(self):
		return type(self)._labels

	def equals(self, element):
		return isinstance(element, type(self)) and self._id == element.getId()

	def getInitialWorld(self):
		return NetDiffWorld(self.getLabels())

	def __hash__(self):
		return super().__hash__()