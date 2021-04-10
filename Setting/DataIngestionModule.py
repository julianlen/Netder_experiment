from typing import List

import networkx as nx
import pandas as pd
from Ontological.Atom import Atom
from Ontological.Constant import Constant
from Ontological.NetDERTGD import NetDERTGD
from Ontological.Variable import Variable


class OntologicalReasoningModule:
    tgd_rules: List[NetDERTGD]
    _atoms: List[Atom]

    def __init__(self):
        self.tgd_rules = []
        self._atoms = []

    def add_atom(self, atom):
        # TODO: Become _atoms a dictionary or a kind of tree k:v = id:dict(term,dict(term..))
        self._atoms.append(atom)

    def add_tgd(self, tgd_rule):
        self.tgd_rules.append(tgd_rule)

    def get_atoms(self):
        return self._atoms


class NetworkDiffusionModule:

    def set_graph(self, digested_dataset):
        # Constructs a Multi graph where nodes are addresses and edges are
        # transaction with blockNumber, transactionHash, value and gasPrice.
        self._diff_graph = nx.from_pandas_edgelist(digested_dataset, 'from', 'to',
                                                   edge_attr=['blockNumber', 'transactionHash', 'value', 'gasPrice'],
                                                   create_using=nx.MultiGraph())


class DataIngestionModule:
    _ndm: NetworkDiffusionModule
    _orm: OntologicalReasoningModule
    _dataset: pd.DataFrame

    def __init__(self, initial_dataset, ontological_reasoning_module, network_diffusion_module):
        self._dataset = pd.read_csv(initial_dataset)
        self._orm = ontological_reasoning_module
        self._ndm = network_diffusion_module
        self.ingest_data()

    # Ingest data will return data processed to be consumed for the knowledge database as in the diffusion graph
    def ingest_data(self):
        # Read dataset from csv_graph_location
        self._dataset = self._dataset.drop(columns=['timestamp', 'callingFunction'])
        self._dataset = self._dataset[self._dataset["isError"] == 'None']
        self._dataset = self._dataset[self._dataset['to'] != 'None']

        addresses = self._dataset['from'].unique()
        for address in addresses:
            self._orm.add_atom(Atom('Account', [Variable('ADD'), Constant(address)]))

        for index, tx in self._dataset.iterrows():
            self._orm.add_atom(
                Atom('GasPrice', [Constant(tx['from']), Constant(tx['blockNumber']), Constant(tx['gasPrice'])]))

        self._ndm.set_graph(self._dataset)

    def get_dataset(self):
        return self._dataset

    def get_orm(self):
        return self._orm

    def get_ndm(self):
        return self.ndm

    def new_data(self, data):
        # append data to _Dataset then ingest it.
        pass
