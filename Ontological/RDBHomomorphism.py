from Ontological.OntQuery import OntQuery
from Ontological.Constant import Constant
from Ontological.Variable import Variable
from Ontological.Atom import Atom
from Ontological.Homomorphism import Homomorphism
from Ontological.Null import Null
from ATLAST.parsing import parser
from ATLAST.codegen.symtable import SymTable
from ATLAST.codegen.ir_generator import IRGenerator
from ATLAST.codegen.sql_generator import SQLGenerator
from ATLAST.dbbackend import schema as schema
import copy
import json
import mariadb
import hashlib

class RDBHomomorphism(Homomorphism):
	NAME = "mapping"
	PK = "1_primary_key"
	def __init__(self, netder_kb):
		self._netder_kb = netder_kb
		#permite almacenar las consultas ya utilizadas
		self._sql_queries = {}
		self._mapping_keys = set()
		self._schema_loc = self._netder_kb.get_schema_loc()


	def to_SQL(self, query):
		string_query = str(query)
		if not (string_query in self._sql_queries):
			result = parser.parse_input(str(query))
			# Set up a symbol table and code generation visitor
		
			symbolTable = SymTable()
			codegenVisitor = IRGenerator(schema.Schema(self._schema_loc))
			sqlGeneratorVisitor = SQLGenerator()
			# Generate the symbol table
			
			result.generateSymbolTable(symbolTable)

			# Perform the code generation into SQLIR using the visitor
			result.accept(codegenVisitor)

			codegenVisitor._IR_stack[0].accept(sqlGeneratorVisitor)
			self._sql_queries[string_query] = sqlGeneratorVisitor._sql

		return self._sql_queries[string_query]
	
	
	#busca todos los posibles mapeos entre un conjunto de atomos (atoms) y una base de datos asociada a la base de conocimiento netder
	#historical_included indica si mapeos que ya hayan sido utilizados pueden ser incluidos en la respuesta
	#el parametro "id_atoms" se utiliza para diferenciar dos conjuntos de atomos sintacticamente iguales pero que corresponden a reglas diferentes
	#esto evita que una vez que un mapeo sea utilizado para el cuerpo de una regla luego ya no pueda ser utilizado para otra con un cuerpo sintacticamente igual
	def get_atoms_mapping(self, atoms, id_atoms = 0, historical_included = False):

		
		exist_var = []
		for atom in atoms:
			pk_variable = atom.get_pk_variable()
			if not pk_variable is None:
				exist_var.append(pk_variable)
		query = OntQuery(exist_var = exist_var, ont_cond = atoms)
		
		sql_query = self.to_SQL(query)
		
		
		var_list = query.get_free_variables()

		data = self._netder_kb.execute(sql_query)
		columns_list = list(data.columns)
		#diccionario para determinar que columna de la consulta SQL (select) va con que variable
		columns_mapping = {}
		for index in range(len(columns_list)):
			if not (columns_list[index] in columns_mapping):
				columns_mapping[columns_list[index]] = {}
				columns_mapping[columns_list[index]]['current_index'] = 0
				columns_mapping[columns_list[index]]['positions'] = [index]
			else:
				columns_mapping[columns_list[index]]['positions'].append(index)


		new_mapping = {}
		for index, row in data.iterrows():
			mapping = {}
			key_mapping = str(id_atoms)
			counter = 0
			key_list = list(row.keys())
			visited = set()
			for key in key_list:
				if not (key in visited):
					current_index = 0
					if key_list.count(key) > 1:
						for i, value in row[key].items():
							#id_var = var_list[counter].getId()
							var_index = columns_mapping[key]['positions'][current_index]
							id_var = var_list[var_index].getId()
							if str(value)[:1] == "z" and str(value)[1:].isdigit():
								mapping[id_var] = Null(value)
							else:
								mapping[id_var] = Constant(value)
							
							key_mapping = key_mapping + '(' + str(id_var) + ',' + str(mapping[id_var]) + ')'
							current_index = current_index + 1
							counter = counter + 1
					else:
						value = row[key]
						#id_var = var_list[counter].getId()
						var_index = columns_mapping[key]['positions'][current_index]
						id_var = var_list[var_index].getId()
						if str(value)[:1] == "z" and str(value)[1:].isdigit():
							mapping[id_var] = Null(value)
						else:
							mapping[id_var] = Constant(value)
						
						key_mapping = key_mapping + '(' + str(id_var) + ',' + str(mapping[id_var]) + ')'
						counter = counter + 1
					visited.add(key)

			key_mapping = key_mapping.encode('utf-8')
			key_mapping = int(hashlib.sha1(key_mapping).hexdigest(), 16)
			

			#consulta para verificar que este mapeo no haya sido utilizado anteriormente
			if not (key_mapping in self._mapping_keys):
				if not (key_mapping in new_mapping):
					new_mapping[key_mapping] = mapping
			elif historical_included:
				new_mapping[key_mapping] = mapping

		return new_mapping

	def save(self, mappings):
		for key_mapping in mappings.keys():
			self._mapping_keys.add(key_mapping)

