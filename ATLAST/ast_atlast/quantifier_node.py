'''
Quantifier Node
A node representing a quantifier followed by a first order logic formula or
another quantifier node.
'''

from .node import Node

class QuantifierNode(Node):
  def __init__(self, lineNo, position, identifiers, formula):
    super(QuantifierNode, self).__init__(lineNo, position)
    self.setChild(0, formula)
    self._identifiers = identifiers
    self._boundValue = None

  def getBoundValue(self):
    return self._boundValue

  def setBoundValue(self, boundValue):
    self._boundValue = boundValue

  def getIdentifiers(self):
    return self._identifiers

