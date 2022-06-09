from Ontological.Variable import Variable
import copy
import hashlib

class Atom:
	def __init__(self, id, terms):
		self._id = id
		self._terms = terms
		#variable que es utilizada para asociar a la clave primaria (basada en el hash del atomo)
		key = 'PK' + str(hash(self))
		#reemplazo "-" por "_" para evitar problemas cuando se traduce a SQL
		key = key.replace("-","_")
		self._pk_variable = Variable(key)

	def getId(self):
		return self._id

	def get_terms(self):
		return self._terms

	def is_mapped(self, atom):
		result = False
		aux_result = True and self._id == atom.getId()
		if (len(self._terms) == len(atom.get_terms())):
			for x in range(0, len(self._terms)):
				aux_result = aux_result and (self._terms[x].getValue() == atom.get_terms()[x].getValue() or (not self._terms[x].isInstanced()))
			result = aux_result

		return result

	def get_mapping(self, atom):
		result = {}
		if self.is_mapped(atom):
			for x in range(0, len(self._terms)):
				if (not self._terms[x].isInstanced()):
					result[self._terms[x].getId()] = atom.get_terms()[x]
				elif (not atom.get_terms()[x].isInstanced()):
					result[atom.get_terms()[x].getId()] = self._terms[x]
		
		return result

	def map(self, mapping):
		success = True
		for i in range(0, len(self._terms)):
			otherTerm = mapping.get(self._terms[i].getId())
			if (otherTerm != None):
				if (self._terms[i].can_be_instanced()):
					self._terms.remove(self._terms[i])
					self._terms.insert(i, otherTerm)
				elif (not self._terms[i].getValue() == otherTerm.getValue()):
					success = False
		return success

	#todos los atomos comienzan con Atom.PK como primer termino porque esta columna se usa como la clave primaria
	#este valor se obtiene de aplicar hash al tomo, pero es algo interno a la implementacion de la BD
	#por esta razon no se considera un termino en si del atomo
	def __str__(self):
		result = self._id + '(' + str(self.get_pk_variable().getId()) + ','
		for term in self._terms:
			result = result + str(term.getId()) + ','

		#saco coma que sobra y agrego parentesis de cierre
		result = result[:-1] + ')'


		return result

	def __eq__(self, atom):
		result = self._id == atom.getId()
		if len(self._terms) == len(atom.get_terms()):
			for x in range(0, len(self._terms)):
				result = result and self._terms[x] == atom.get_terms()[x]
		else:
			result = False

		result = result or (self is atom)
		
		return result

	def __hash__(self):
		string = str(self._id) + ','
		for term in self._terms:
			string = string + str(hash(term)) + ','

		string = string.encode('utf-8')
		return int(hashlib.sha1(string).hexdigest(), 16)

	def is_equivalent(self, atom):
		result = True
		if (self._id == atom.getId()) and (len(self._terms) == len(atom.get_terms())):
			aux_terms = copy.deepcopy(atom.get_terms())
			index = 0
			for term in self._terms:
				if (not (term.getValue() == aux_terms[index].getValue())):
					result = False
					break
				index += 1					
		else:
			result = False


		return result

	def get_pk_variable(self):
		return copy.deepcopy(self._pk_variable)

	#devueve los terminos que son variables
	def get_variables(self):
		result = []
		for term in self._terms:
			if isinstance(term, Variable):
				variable = copy.deepcopy(term)
				result.append(variable)
		return result


	def is_present(self, term):
		result = False
		for my_term in self._terms:
			if my_term.getId() == term.getId():
				result = True
				break
		return result

