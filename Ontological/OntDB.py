import copy
from Ontological.Homomorphism import Homomorphism

# This class is a database for atoms.
# _atoms = dict{atom_id : [atom]}


class OntDB:

	def __init__(self, ont=None):
		if ont is None:
			ont = []
		self._atoms = {}
		self._size = len(ont)
		for atom in ont: 	# It fills _atoms
			if atom.getId() in self._atoms.keys():
				self._atoms[atom.getId()].append(atom)
			else:
				self._atoms[atom.getId()] = [atom]

	def get_atoms_from_pred(self, pred):
		result = []
		if pred in self._atoms.keys():
			result = self._atoms[pred]

		return result

	def remove_atoms_from_pred(self, pred):
		if pred in self._atoms.keys():
			length = len(self._atoms[pred])
			if length > 0:
				self._atoms[pred] = []
				self._size -= length

	def add_atom(self, atom):
		atom_added = True
		if atom.getId() in self._atoms.keys():
			if atom not in self._atoms[atom.getId()]:
				self._atoms[atom.getId()].append(atom)
				self._size += 1
			else:
				return False  # The atom has been already added
		else:
			self._atoms[atom.getId()] = [atom]
			self._size += 1

		return atom_added

	def apply_mapping(self, mapping):
		aux_atoms = {}
		for key in self._atoms.keys():
			aux_ont_data = []
			for atom in self._atoms[key]:
				atom.map(mapping)
				if atom not in aux_ont_data:
					aux_ont_data.append(atom)
			aux_atoms[key] = aux_ont_data

		self._atoms = aux_atoms

	def get_atoms(self):
		return self._atoms

	def get_size(self):
		return self._size

	def _get_equivalent_index(self, atoms, atom):
		result = None
		if atom.getId() in atoms.keys():
			index = 0
			for other_atom in atoms[atom.getId()]:
				if other_atom.is_equivalent(atom):
					result = index
					break
				index += 1

		return result

	def is_equivalent(self, ont_db):
		result = False
		if self._size == ont_db.get_size():
			success = True
			other_atoms = copy.deepcopy(ont_db.get_atoms())
			my_atoms = copy.deepcopy(self._atoms)
			db1 = []
			db2 = []
			for key in other_atoms.keys():
				for atom in other_atoms[key]:
					index = self._get_equivalent_index(my_atoms, atom)
					if not index is None:
						my_atoms[key] = my_atoms[key][:index] + my_atoms[key][index + 1:]
					else:
						success = False
						break
				if key in self._atoms.keys():
					db1 = db1 + self._atoms[key]
				db2 = db2 + other_atoms[key]
			if success:
				h = Homomorphism()
				mapping = h.get_atoms_mapping(db1, db2)
				if len(mapping) > 0:

					for key_pos in mapping:
						clone_db1 = copy.deepcopy(db1)
						clone_db2 = copy.deepcopy(db2)

						for atom in clone_db1:
							atom.map(mapping[key_pos])

						for atom in clone_db2:
							atom.map(mapping[key_pos])

						aux_result = True
						for atom1 in clone_db1:
							found = None
							for atom2 in clone_db2:
								if atom2 == atom1:
									found = atom2
							if not found is None:
								clone_db2.remove(found)
							else:
								aux_result = False
								break
						if aux_result:
							result = True
		return result
