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


	def to_SQL(self, query):
		result = parser.parse_input(str(query))
		# Set up a symbol table and code generation visitor
		
		symbolTable = SymTable()
		codegenVisitor = IRGenerator(schema.Schema())
		sqlGeneratorVisitor = SQLGenerator()
		# Generate the symbol table
		
		result.generateSymbolTable(symbolTable)

		# Perform the code generation into SQLIR using the visitor
		result.accept(codegenVisitor)

		codegenVisitor._IR_stack[0].accept(sqlGeneratorVisitor)

		return sqlGeneratorVisitor._sql
	
	
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
		
		var_set = query.get_free_variables()

		con = self._netder_kb.get_connection()
		cur = con.cursor()

		cur.execute(sql_query)
		data = cur.fetchall()

		new_mapping = {}
		for row in data:
			mapping = {}
			key_mapping = str(id_atoms)
			index = 0
			for var in var_set:
				key = var.getId()
				value = str(row[index])
				if value[:1] == "z" and value[1:].isdigit():
					mapping[key] = Null(value)
				else:
					mapping[key] = Constant(value)
				
				key_mapping = key_mapping + '(' + str(key) + ',' + str(mapping[key]) + ')'
				index = index + 1

			key_mapping = key_mapping.encode('utf-8')
			key_mapping = int(hashlib.sha1(key_mapping).hexdigest(), 16)
			

			#consulta para verificar que este mapeo no haya sido utilizado anteriormente
			mapping_query = "SELECT * FROM " + RDBHomomorphism.NAME + " WHERE " + RDBHomomorphism.PK + "=" + str(key_mapping) + ";"
			cur.execute(mapping_query)
			if len(cur.fetchall()) == 0:
				if not (key_mapping in new_mapping):
					new_mapping[key_mapping] = mapping
			elif historical_included:
				new_mapping[key_mapping] = mapping

		con.commit()
		con.close()


		return new_mapping

	def save(self, mappings):
		con = self._netder_kb.get_connection()
		cur = con.cursor()
		columns = self._netder_kb.get_columns(con, RDBHomomorphism.NAME)
		#nombre de la columna correspondiente a la clave primaria
		pk = columns[0][0]
		mapping_insert_ini = "INSERT INTO " + RDBHomomorphism.NAME + " VALUES "
		mapping_insert_partial = ""
		for key_mapping in mappings.keys():
			#consulta para verificar que la clave del homomorfismo aun no esta en la base de datos
			cur.execute("SELECT * FROM " + RDBHomomorphism.NAME + " WHERE " + pk + "='" + str(key_mapping) + "';")
			data = cur.fetchall()
			if not (len(data) > 0) :
				mapping_insert_partial = mapping_insert_partial + "(" + str(key_mapping) + "),"

		if len(mapping_insert_partial) > 0:
			#saco la coma demas y agrego un punto y coma
			mapping_insert_partial = mapping_insert_partial[:-1] + ";"
			mapping_insert = mapping_insert_ini + mapping_insert_partial
			cur.execute(mapping_insert)

		con.commit()
		con.close()

