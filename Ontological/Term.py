import hashlib
class Term:

	def __init__(self, id, value = None):
		self._id = str(id)
		self._value = value

	def getId(self):
		return self._id

	def getValue(self):
		return self._value

	def setValue(self, value):
		self._value = value

	def setId(self, id):
		self._id = id

	def __hash__(self):
		mystring = str(self._id)
		mystring = mystring.encode('utf-8')
		return int(hashlib.sha1(mystring).hexdigest(), 16)

	def __eq__(self, term):
		result = False
		if term is None:
			result = (self is term)
		else:
			result = (self._id == term.getId())

		return result

	def isInstanced(self):
		return self._value != None

	def can_be_instanced(self):
		return not self.isInstanced()

	def __str__(self):
		return "id: " + str(self._id) + " value: " + str(self._value)