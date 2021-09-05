from Ontological.Term import Term
import hashlib

class Constant(Term):

	def __init__(self, value):
		super().__init__(value, value)
		self._id = "'" + str(value) + "'"

	def __hash__(self):
		mystring = str(self._value)
		mystring = mystring.encode('utf-8')
		return int(hashlib.sha1(mystring).hexdigest(), 16)

	def __str__(self):
		return str(self._value)
