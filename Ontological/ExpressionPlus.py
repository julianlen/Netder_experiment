from Ontological.Term import Term
import hashlib

class ExpressionPlus:

	def __init__(self, terms):
		self._terms = terms
		self._id = str(self)

	def __str__(self):
		str_terms = []
		for term in self._terms:
			# print('type(term)', type(term))
			# print('str(term)', str(term))
			str_terms.append(str(term))

		result = ' + '.join(str_terms)
		# print('result', result)
		return result

	def getId(self):
		return self._id

	def getTerms(self):
		return self._terms