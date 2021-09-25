import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Ontological.Term import Term
import hashlib

class Variable(Term):

	def __hash__(self):
		result = None
		if self._value is None:
			result = super().__hash__()
		else:
			mystring = str(self._value)
			mystring = mystring.encode('utf-8')
			result = int(hashlib.sha1(mystring).hexdigest(), 16)
		return result
		# return int(hashlib.sha1(string).hexdigest(), 16)

	def __str__(self):
		# print('---------')
		# print('self._id', self._id)
		return str(self._id)