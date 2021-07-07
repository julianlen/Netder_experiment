from Ontological.Atom import Atom

class Distinct(Atom):

	def __init__(self, value1, value2):
		super().__init__('distinct', [value1, value2])
		self._pk_variable = None

	#se puede mapear a cualquier atomo si los dos terminos de este atomo estan instanciados y estos son diferentes o
	#los terminos de este atomo no estan instanciados y el parametro "atom" tiene "id" diferente (osea no es "distinct")
	def is_mapped(self, atom):
		result = False
		#result = (self._terms[0].isInstanced() and self._terms[1].isInstanced()) and (self._terms[0].getValue() != self._terms[1].getValue())
		result = (self._terms[0].isInstanced() and self._terms[1].isInstanced()) and (self._terms[0].getValue() != self._terms[1].getValue())
		result = result or (not (self._terms[0].isInstanced() and self._terms[1].isInstanced()) and self._id != atom.getId())
		return result

	def get_mapping(self, atom):
		return {}

	def __str__(self):
		return self._terms[0].getId() + '≠' + self._terms[1].getId()

	def get_variables(self):
		return []