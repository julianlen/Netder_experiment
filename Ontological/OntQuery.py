class OntQuery:

	def __init__(self, exist_var = [], ont_cond = []):
		self._exist_var = []
		self._ont_cond = ont_cond
		for var in exist_var:
			for atom in self._ont_cond:
				if var in atom.get_terms():
					self._exist_var.append(var)
					break

	def get_exist_var(self):
		return self._exist_var

	def get_ont_body(self):
		return self._ont_cond

	def get_variables(self):
		result = {}
		for atom in self._ont_cond:
			for var in atom.get_variables():
				result[var.getId()] = var
		result = result.values()

		return list(result)

	def __str__(self):
		result = ''
		if len(self._exist_var) > 0:
			result = result + '∃'
			for var in self._exist_var:
				result = result + var.getId() + ','
			#saco la coma que sobra
			result = result[:-1]
		
		result = result + '('

		for atom in self._ont_cond:
			result = result + str(atom) + '∧'

		#saco ∧ que sobra y agrego de perentesis de cierre
		result = result[:-1] + ')'

		return result


