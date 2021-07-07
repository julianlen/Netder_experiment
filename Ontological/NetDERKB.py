import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import copy
import json
import portion
import pandas as pd
import pandasql as ps
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.Atom import Atom
from Ontological.Null import Null
from Ontological.RDBHomomorphism import RDBHomomorphism
from Diffusion_Process.NetDiffFact import NetDiffFact
from Diffusion_Process.NLocalLabel import NLocalLabel
from Diffusion_Process.ELocalLabel import ELocalLabel
from Diffusion_Process.NetDiffNode import NetDiffNode
from Diffusion_Process.NetDiffEdge import NetDiffEdge
from Diffusion_Process.NetDiffGraph import NetDiffGraph
from ATLAST.dbbackend.schema import Schema

class NetDERKB:
	NULL_INFO = "null_info"
	def __init__(self, data = set(), net_diff_graph = None, schema_loc = None , netder_tgds=[], netder_egds = [], netdiff_lrules=[], netdiff_grules=[]):
		self._schema_loc = schema_loc
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

		self._load_schema()
		self._create_tables()
		self.add_ont_data(data)

	def _load_schema(self):
		self._db_schema = {}
		tables_info = Schema(self._schema_loc).getAllData()['tables']
		for name in tables_info.keys():
			self._db_schema[name] = {'columns':[]}
			for pk in tables_info[name]['primary_keys']:
				clean = pk.replace("\t", "")
				clean = clean.replace("\n", "")
				self._db_schema[name]['columns'].append(clean)


	def _create_tables(self):
		self._tables = {}
		self._tables[NetDiffNode.ID] = pd.DataFrame(columns = ["_1_primary_key", "_2_id"])
		self._tables[NetDiffEdge.ID] = pd.DataFrame(columns = ["_1_primary_key", "_2_from", "_3_to"])
		self._tables[NetDiffFact.ID] = pd.DataFrame(columns = ["_1_primary_key", "_2_component", "_3_label", "_4_interval_lower", "_5_interval_upper", "_6_t_lower", "_7_t_lower"])
		self._tables[NetDERKB.NULL_INFO] = pd.DataFrame(columns = ["_1_primary_key", "_2_value", "_3_table_name", "_4_foreign_key"])
		for table_name in self._db_schema.keys():
			table_info = self._db_schema[table_name]
			columns = table_info["columns"]
			self._tables[table_name] = pd.DataFrame(columns = columns)

	def add_ont_data(self, atoms):
		success = False
		for atom in atoms:
			#si el atomo no se encuentra en la base de datos significa que puede ser agregado
			df = None
			columns = self.get_columns(atom.getId())
			
			df = self._tables[atom.getId()]
			if len(df.loc[df[columns[0]] == hash(atom)]) == 0:
				terms = atom.get_terms()
				new_row = {}
				new_row[columns[0]] = hash(atom)
				for index in range(0, len(terms)):
					new_row[columns[index + 1]] = terms[index].getValue()

				self._tables[atom.getId()] = df.append(new_row, ignore_index=True)
				success = True

		return success


	def get_net_diff_facts(self):
		result = set()
		df = self._tables[NetDiffFact.ID]
		columns = self.get_columns(NetDiffFact.ID)
		node_columns = self.get_columns(NetDiffNode.ID)
		edge_columns = self.get_columns(NetDiffEdge.ID)
		for index, row in df.iterrows():
			comp_hash = row[columns[1]]
			comp = None
			label = None
			df_node = self._tables[NetDiffNode.ID]
			node_id = None
			for i, r in df_node.loc[df_node["_1_primary_key"] == comp_hash].iterrows():
				node_id = r[node_columns[1]]

			if not (node_id is None):
				comp = NetDiffNode(node_id)
				label = NLocalLabel(row[columns[2]])
			else:
				df_edge = self._tables[NetDiffEdge.ID]
				edge_from = None
				edge_to = None
				for i, r in df_edge.loc[df_edge["_1_primary_key"] == comp_hash].iterrows():
					edge_from = r[edge_columns[1]]
					edge_to = r[edge_columns[2]]

				if not(edge_from is None and edge_to is None):
					comp = NetDiffEdge(edge_from, edge_to)
					label = ELocalLabel(row[columns[2]])
			
			result.add(NetDiffFact(comp, label, portion.closed(float(row[columns[3]]), float(row[columns[4]])), int(row[columns[5]]), int(row[columns[6]])))

		return result

	def get_columns(self, table_name):
		#columns = self._db_schema[table_name]['columns']
		columns = list(self._tables[table_name].columns)
		return columns


	def create_null(self):
		result = Null()
		return result

	def save_null_info(self, atom, null):
		if atom.is_present(null):
			null_info_atom = Atom(str(NetDERKB.NULL_INFO), [Constant(null.getValue()), Constant(str(atom.getId())), Constant(hash(atom))])
			self.add_ont_data({null_info_atom})

	def exists(self, atom):
		columns = self.get_columns(atom.getId())
		#key_column = "1_primary_key"
		key_column = columns[0]
		df = self._tables[atom.getId()]
		result = len(df.loc[df[key_column] == hash(atom)]) > 0
		return result

	def update_nulls(self, mapping):
		success = False
		if len(mapping) > 0:
			
			nulls_info_columns = self.get_columns(NetDERKB.NULL_INFO)
			for key in mapping.keys():
				condicion1 = isinstance(mapping[key], Null)
				condicion2 = isinstance(mapping[key], Constant)
				if condicion1 or condicion2:
					df_null_info = self._tables[NetDERKB.NULL_INFO]
					nulls_info = df_null_info.loc[df_null_info[nulls_info_columns[1]] == key]
					for index, row in nulls_info.iterrows():
						table_name = row[nulls_info_columns[2]]
						columns = self.get_columns(table_name)
						pk_col = columns[0]
						#saco la primer columna que es relativa a la clave primaria
						other_columns = columns[1:]
						fk = row[nulls_info_columns[3]]
						df_table_name = self._tables[table_name]
						atom_row = df_table_name.loc[df_table_name[pk_col] == fk]
						terms = []
						for index_ar, item_ar in atom_row.iterrows():
							for col in other_columns:
								item = item_ar[col]
								if str(item)[:1] == "z" and str(item)[1:].isdigit():
									terms.append(Null(str(item)))
								else:
									terms.append(Constant(item))

						atom = Atom(table_name, terms)
						#hash before mapping
						hash_bm = hash(atom)
						atom.map(mapping)
						if condicion1:
							if not self.exists(atom):
								for index_ar, item_ar in atom_row.iterrows():
									counter = 0
									for col in other_columns:
										df_table_name.at[index_ar, col] = atom.get_terms()[counter].getValue()
										counter += 1
								df_null_info.at[index, nulls_info_columns[1]] = mapping[key].getValue()
							else:
								for index_ar, item_ar in df_table_name.loc[df_table_name[pk_col] == hash_bm].iterrows():
									self._tables[table_name] = df_table_name.drop(index_ar)

								for index_ar, item_ar in df_null_info.loc[df_null_info[nulls_info_columns[3]] == hash_bm].iterrows():
									self._tables[NetDERKB.NULL_INFO] = df_null_info.drop(index_ar)
						elif condicion2:
							self.add_ont_data({atom})

			success = True

		return success
		

	def get_net_diff_graph(self):
		nodes_df = self._tables[NetDiffNode.ID]
		columns = self.get_columns(NetDiffNode.ID)
		nodes = []
		for index, row in nodes_df.iterrows():
			nodes.append(NetDiffNode(row[columns[1]]))

		edges_df = self._tables[NetDiffEdge.ID]
		columns = self.get_columns(NetDiffEdge.ID)
		edges = []
		for index, row in edges_df.iterrows():
			edges.append(NetDiffEdge(row[columns[1]], row[columns[2]]))

		net_diff_graph = NetDiffGraph("graph", nodes, edges)

		return net_diff_graph

	def execute(self, sql_query):
		for key in self._tables.keys():
			locals()[key] = self._tables[key]

		return ps.sqldf(sql_query, locals())


	def get_netder_egds(self):
		return self._netder_egds

	def get_netder_tgds(self):
		return self._netder_tgds

	def get_net_diff_lrules(self):
		return self._netdiff_lrules

	def get_net_diff_grules(self):
		return self._netdiff_grules

	def get_schema_loc(self):
		return self._schema_loc

