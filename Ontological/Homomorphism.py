import copy


class Homomorphism:

    def get_atom_mapping(self, atom, data_base):
        result = {}
        for data in data_base:
            if atom.is_mapped(data):
                mapping = atom.get_mapping(data)
                key_mapping = ""
                for key in mapping.keys():
                    key_mapping = key_mapping + '(' + str(key) + ',' + str(mapping[key]) + ')'
                result[key_mapping] = mapping
                if mapping == {}:
                    break
        if len(result) == 0:
            result = None

        return result

    def get_atoms_mapping(self, atoms, data_base):
        aux_result = {'': {}}
        aux_mapped_atoms = {'': copy.deepcopy(atoms)}

        for index in range(0, len(atoms)):
            mapped_atoms = aux_mapped_atoms
            result = aux_result
            aux_mapped_atoms = {}
            aux_result = {}
            for ma_key in mapped_atoms.keys():
                mapping = self.get_atom_mapping(mapped_atoms[ma_key][index], data_base)
                if mapping is not None:
                    for mapping_key in mapping.keys():
                        other_mapping = copy.deepcopy(result[ma_key])
                        other_mapping.update(mapping[mapping_key])
                        aux_result[ma_key + mapping_key] = other_mapping
                        cloned_mapped_atoms = copy.deepcopy(mapped_atoms[ma_key])
                        for otherAtom in cloned_mapped_atoms:
                            otherAtom.map(mapping[mapping_key])
                        aux_mapped_atoms[ma_key + mapping_key] = cloned_mapped_atoms
        return aux_result
