import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import copy
import json
import mariadb
import portion
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.Atom import Atom
from Ontological.Null import Null
from Diffusion_Process.NetDiffFact import NetDiffFact
from Diffusion_Process.NLocalLabel import NLocalLabel
from Diffusion_Process.ELocalLabel import ELocalLabel
from Diffusion_Process.NetDiffNode import NetDiffNode
from Diffusion_Process.NetDiffEdge import NetDiffEdge

class NetDERKB:
	NULL_INFO = "null_info"
	def __init__(self, data = set(), net_diff_graph = None, config_db = "config_db.json", netder_tgds=[], netder_egds = [], netdiff_lrules=[], netdiff_grules=[]):
		self._config_db = config_db
		self._netder_tgds = netder_tgds
		self._netder_egds = netder_egds
		aux_rules = self._netder_tgds + self._netder_egds
		counter = 0
		for rule in aux_rules:
			rule.set_id(counter)
			counter = counter + 1
		self._netdiff_lrules = netdiff_lrules
		self._netdiff_grules = netdiff_grules
		self._net_diff_graph = net_diff_graph
		nodes = net_diff_graph.getNodes()
		edges = net_diff_graph.getEdges()
		data = data.union(nodes)
		data = data.union(edges)
		self.add_ont_data(data)


	def get_config_db(self):
		return self._config_db

	def get_connection(self):
		with open(self._config_db) as config_json:
			config_data = json.load(config_json)
		try:
		    con = mariadb.connect(
		        user=config_data['user'],
		        password=config_data['password'],
		        host=config_data['host'],
		        port=config_data['port'],
		        database=config_data['database']
		        )
		except mariadb.Error as e:
			print(f"Error connecting to MariaDB Platform: {e}")
			sys.exit(1)

		return con


	def add_ont_data(self, atoms):
		con = self.get_connection()
		cur = con.cursor()

		#filtrar los atomos que ya se encuentran en la base de datos
		select_ini = 'SELECT * FROM '
		filtered_atoms = []
		for atom in atoms:
			#consulta para obtener los nombres de las columnas de la tabla relativa al atomo "atom"
			column_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+ atom.getId() + "' ORDER BY ORDINAL_POSITION"
			cur.execute(column_query)
			columnas = cur.fetchall()
			index = 0
			select_query = select_ini + atom.getId() + ' WHERE '
			terms = atom.get_terms()
			'''
			for columna in columnas:
				select_query = select_query + columna[0] + " = " + terms[index].getId() + " AND "
				index += 1'''
			#saco el AND y espacios demas
			#select_query = select_query[:-5]
			#la primer columna es el nombre de la clave primaria, cuyo valor es el hash del atomo
			pk_name = columnas[0][0]
			select_query = select_query + pk_name + " = " + str(hash(atom))
			
			cur.execute(select_query)
			data = cur.fetchall()
			#si el atomo no se encuentra en la base de datos significa que puede ser agregado
			if len(data) == 0:
				filtered_atoms.append(atom)

		success = None
		if len(filtered_atoms) > 0:
			sql_query_ini = 'INSERT INTO '
			sql_queries_part = {}
			for atom in filtered_atoms:
				if not (atom.getId() in sql_queries_part):
					sql_queries_part[atom.getId()] = ' VALUES '
				
				sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '(' + "'" + str(hash(atom)) + "',"
				for term in atom.get_terms():
					sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '\'' + str(term.getValue()) + '\'' + ','
				#saco la coma que queda demas
				sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()][:-1]
				sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '),'

			for key in sql_queries_part.keys():
				#saco la coma que queda demas
				sql_queries_part[key] = sql_queries_part[key][:-2]
				sql_queries_part[key] = sql_queries_part[key] + ')'
				sql_query = sql_query_ini + '`' + key + '`' + sql_queries_part[key]
	
				cur.execute(sql_query)
			success = True
		else:
			success = False

		con.commit()
		con.close()

		return success



	def add_net_knowledge(self, knowledge, time):
		self._net_db.add_knowledge(knowledge, time)

	def add_facts(self, facts):
		self._net_db.add_facts(facts)

	def get_net_diff_facts(self):
		result = set()
		con = self.get_connection()
		cur = con.cursor()
		tables = {"net_diff_fact": {"name":NetDiffFact.ID}, "node": {"name":NetDiffNode.ID}, "edge": {"name":NetDiffEdge.ID}}
		for key in tables.keys():
			column_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+ tables[key]["name"] + "' ORDER BY ORDINAL_POSITION"
			cur.execute(column_query)
			columns = cur.fetchall()
			tables[key]["pk_col"] = columns[0][0]
			#saco la columna de la clave primaria y me quedo solo con las restantes
			tables[key]["other_col"] = columns[1:]

		#columna que es clave foranea en net_diff_fact (hace referencia a un nodo o arco)
		tables["net_diff_fact"]["fk_col"] = tables["net_diff_fact"]["other_col"][0][0]
		#la clave foranea no va a ser incluida en el select/proyeccion de la consulta SQL
		tables["net_diff_fact"]["other_col"] = tables["net_diff_fact"]["other_col"][1:]		
		
		query_ini = "SELECT "
		#primero se obtenian los net_diff_facts relativos a nodos
		query = query_ini
		for column in tables["node"]["other_col"]:
			query = query + column[0] + ","

		for column in tables["net_diff_fact"]["other_col"]:
			query = query + column[0] + ","
		
		#saco la coma demas
		query = query[:-1]
		query = query + " FROM " + tables["node"]["name"] + " INNER JOIN " + tables["net_diff_fact"]["name"] + " ON " + tables["node"]["name"] + "." + tables["node"]["pk_col"] + "=" + tables["net_diff_fact"]["name"] + "." + tables["net_diff_fact"]["fk_col"]
		
		cur.execute(query)
		data = cur.fetchall()
		for row in data:
			node = NetDiffNode(row[0])
			result.add(NetDiffFact(node, NLocalLabel(row[1]), portion.closed(float(row[2]), float(row[3])), int(row[4]), int(row[5])))

		#primero se obtenian los net_diff_facts relativos a arcos

		query = query_ini
		for column in tables["edge"]["other_col"]:
			query = query + column[0] + ","

		for column in tables["net_diff_fact"]["other_col"]:
			query = query + column[0] + ","
		
		#saco la coma demas
		query = query[:-1]
		query = query + " FROM " + tables["edge"]["name"] + " INNER JOIN " + tables["net_diff_fact"]["name"] + " ON " + tables["edge"]["name"] + "." + tables["edge"]["pk_col"] + "=" + tables["net_diff_fact"]["name"] + "." + tables["net_diff_fact"]["fk_col"]
		
		cur.execute(query)
		data = cur.fetchall()
		for row in data:
			edge = NetDiffEdge(row[0], row[1])
			result.add(NetDiffFact(edge, ELocalLabel(row[1]), portion.closed(float(row[2]), float(row[3])), int(row[4]), int(row[5])))

		con.commit()
		con.close()

		return result

	def _get_columns(self, table_name):
		con = self.get_connection()
		cur = con.cursor()
		#consulta para obtener los nombres de las columnas de la tabla "null_info"
		column_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+ str(table_name) + "' ORDER BY ORDINAL_POSITION"
		
		cur.execute(column_query)
		columns = cur.fetchall()
		con.commit()
		con.close()
		return columns

	def create_null(self):
		result = None
		con = self.get_connection()
		cur = con.cursor()
		
		columns = self._get_columns(NetDERKB.NULL_INFO)
		#consulta para verificar cuantos valores nulls distintos ya fueron creados
		query = "SELECT DISTINCT " + columns[1][0] + " FROM " + NetDERKB.NULL_INFO + ";"
		cur.execute(query)
		data = cur.fetchall()
		result = Null("z" + str(len(data)))
		con.commit()
		con.close()
		return result

	def save_null_info(self, atom, null):
		if atom.is_present(null):
			null_info_atom = Atom(str(NetDERKB.NULL_INFO), [Constant(str(null.getValue())), Constant(str(atom.getId())), Constant(str(hash(atom)))])
			self.add_ont_data({null_info_atom})

	def update_nulls(self, mapping):
		if len(mapping) > 0:
			
			con = self.get_connection()
			cur = con.cursor()
			
			nulls_info_columns = self._get_columns(NetDERKB.NULL_INFO)
			for key in mapping.keys():
				condicion1 = isinstance(mapping[key], Null)
				condicion2 = isinstance(mapping[key], Constant)
				if condicion1 or condicion2:
					query_info_nulls = "SELECT * FROM " + NetDERKB.NULL_INFO + " WHERE " + nulls_info_columns[1][0] + "= '" + str(key) + "';"
					cur.execute(query_info_nulls)
					nulls_info = cur.fetchall()
					for row in nulls_info:
						columns = self._get_columns(row[2])
						pk_col = columns[0][0]
						#saco la primer columna que es relativa a la clave primaria
						other_columns = columns[1:]
						fk = row[3]
						table_name = row[2]
						query = "SELECT * FROM " + table_name + " WHERE " + pk_col + "=" + fk + ";"
						cur.execute(query)
						data = cur.fetchall()
						terms = []
						#elimino el primer dato porque es relativo a la clave de la tabla (hash)
						data[0] = data[0][1:]
						for item in data[0]:
							if item[:1] == "z" and item[1:].isdigit():
								terms.append(Null(str(item)))
							else:
								terms.append(Constant(str(item)))
							
						atom = Atom(table_name, terms)
						atom.map(mapping)
						if condicion1:
							query_update_ini = "UPDATE "
							query_update = query_update_ini + table_name + " SET "
							for index in range(len(other_columns)):
								query_update = query_update + other_columns[index][0] + "='" + atom.get_terms()[index].getValue() + "', "

							#quito la coma y el espacio demas
							query_update = query_update[:-2]
							query_update = query_update + " WHERE " + pk_col + "=" + fk + ";"
							
							cur.execute(query_update)

						elif condicion2:
							self.add_ont_data({atom})

					if condicion1:
						query_delete = "DELETE FROM " + NetDERKB.NULL_INFO + " WHERE " + nulls_info_columns[1][0] + "='" + str(key) + "';"
						cur.execute(query_delete)
			con.commit()
			con.close()
		

	def get_net_diff_graph(self):
		'''NetDiffNode.ID
		NetDiffEdge.ID'''

		return self._net_diff_graph


	def get_netder_egds(self):
		return self._netder_egds

	def get_netder_tgds(self):
		return self._netder_tgds

	def get_net_diff_lrules(self):
		return self._netdiff_lrules

	def get_net_diff_grules(self):
		return self._netdiff_grules

