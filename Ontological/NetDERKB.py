import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import copy
import json
import mariadb
from Ontological.NetDB import NetDB
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.Atom import Atom
from Ontological.OntDB import OntDB

class NetDERKB:

	def __init__(self, ont_data = ([], ""), net_db= NetDB(), netder_tgds=[], netder_egds = [], netdiff_lrules=[], netdiff_grules=[]):
		self._ont_db = OntDB()
		self.add_ont_knowledge(ont_data[0])
		self._config_db = ont_data[1]
		self._net_db = net_db
		self._netder_tgds = netder_tgds
		self._netder_egds = netder_egds
		self._netdiff_lrules = netdiff_lrules
		self._netdiff_grules = netdiff_grules


	def get_config_db(self):
		return self._config_db

	def add_ont_knowledge(self, atoms):
		success = False
		copy_atoms = copy.deepcopy(atoms)
		index = 0
		for atom in copy_atoms:
			result = self._ont_db.add_atom(atom)
			success = success or result

		return success

	def add_ont_data(self, atoms):

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

		cur = con.cursor()

		sql_query_ini = 'INSERT INTO '
		sql_queries_part = {}
		for atom in atoms:
			if not (atom.getId() in sql_queries_part):
				sql_queries_part[atom.getId()] = ' VALUES '
			
			sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '('
			for term in atom.get_terms():
				sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '\'' + term.getValue() + '\'' + ','
			#saco la coma que queda demas
			sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()][:-1]
			sql_queries_part[atom.getId()] = sql_queries_part[atom.getId()] + '),'

		for key in sql_queries_part.keys():
			#saco la coma que queda demas
			sql_queries_part[key] = sql_queries_part[key][:-2]
			sql_queries_part[key] = sql_queries_part[key] + ')'
			sql_query = sql_query_ini + '`' + key + '`' + sql_queries_part[key]
			print('sql_query', sql_query)
			cur.execute(sql_query)

		con.commit()
		con.close()



	def add_net_knowledge(self, knowledge, time):
		self._net_db.add_knowledge(knowledge, time)

	def add_facts(self, facts):
		self._net_db.add_facts(facts)

	def get_net_diff_facts(self):
		return self._net_db.get_net_diff_facts()

	def get_ont_db(self):
		return self._ont_db

	def get_net_data(self):
		return self._net_db.get_net_data()

	def get_comp_from_atom(self, atom):
		return self._net_db.get_comp_from_atom(atom)

	def get_netder_egds(self):
		return self._netder_egds

	def get_netder_tgds(self):
		return self._netder_tgds

	def get_net_diff_lrules(self):
		return self._netdiff_lrules

	def get_net_diff_grules(self):
		return self._netdiff_grules

	def get_net_diff_graph(self):
		return self._net_db.get_net_diff_graph()

	def apply_map(self, mapping):
		self._ont_db.apply_mapping(mapping)

	def get_data_from_pred(self, pred):
		return self._ont_db.get_atoms_from_pred(pred) + self._net_db.get_comp_from_pred(pred)


	def remove_atoms_from_pred(self, pred):
		self._ont_db.remove_atoms_from_pred(pred)

	def get_net_db(self):
		return self._net_db