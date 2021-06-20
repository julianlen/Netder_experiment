from datetime import datetime
from typing import List
import pandas as pd
import numpy as np
from Ontological.Atom import Atom
from Ontological.Constant import Constant
from Ontological.NetDERKB import NetDERKB


class DataIngestionModule:
    _knowledge_base: NetDERKB
    _dataset: pd.DataFrame

    def __init__(self, initial_dataset, sub_dataset, knowledge_base):
        self._dataset = pd.read_csv(initial_dataset)
        self._knowledge_base = knowledge_base
        self.ingest_data(sub_dataset)

    # Ingest data will return data processed to be consumed for the knowledge database as in the diffusion graph
    def ingest_data(self, _range):
        # Read dataset from csv_graph_location
        self._dataset['value'] = self._dataset['value'].astype(float)
        self._dataset = self._dataset.drop(columns=['timestamp', 'callingFunction'])
        self._dataset = self._dataset[self._dataset["isError"] == 'None']
        self._dataset = self._dataset[self._dataset['to'] != 'None']
        print('Leyendo dataset')
        #   Split dataset in sub datasets by adding a new column SD
        self._dataset = self.get_sub_datasets(self._dataset, _range)

        #   Creating atoms given all the addresses
        addresses = self._dataset['from'].unique()

        idx = 0
        for index, transaction in self._dataset.iterrows():
            print(idx)
            address_ = transaction['from']
            sub_dataset_ = transaction['sd']
            block_number_ = transaction['blockNumber']
            gas_price = transaction['gasPrice']
            recipient_ = transaction['to']
            balance_ = transaction['value']

            self.add_atom(Atom('Account', [Constant(address_)]))
            self.add_atom(Atom('Account', [Constant(recipient_)]))

            self.add_atom(
                Atom('Transaction', [Constant(address_), Constant(sub_dataset_), Constant(recipient_),
                                     Constant(block_number_), Constant(gas_price),
                                     Constant(balance_)]))
            self.add_atom(
                Atom('GasPrice', [Constant(sub_dataset_), Constant(address_), Constant(block_number_),
                                  Constant(gas_price)]))
            self.add_atom(
                Atom('Out_Balance', [Constant(sub_dataset_), Constant(address_), Constant(block_number_),
                                     Constant(balance_)]))

            self.add_atom(
                Atom('In_Balance', [Constant(sub_dataset_), Constant(recipient_), Constant(block_number_),
                                    Constant(balance_)]))

            self.add_atom(self.get_in_degree_atom(self._dataset, sub_dataset_, address_, block_number_))
            self.add_atom(self.get_out_degree_atom(self._dataset, sub_dataset_, address_, block_number_))
            # self.add_atom(self.get_attractiveness_atom(self._dataset, sub_dataset_, address_, block_number_))
            self.add_atom(self.get_thr_in_degree_atom(address_, sub_dataset_))
            self.add_atom(self.get_thr_out_degree_atom(address_, sub_dataset_))
            self.add_atom(self.get_thr_in_bal_atom(address_, sub_dataset_))
            self.add_atom(self.get_thr_out_bal_atom(address_, sub_dataset_))
            self.add_atom(self.get_thr_gas_price(address_, sub_dataset_))
            idx += 1

    def get_thr_gas_price(self, address_, sub_dataset_):
        return Atom('Thr_gas_price', [Constant(address_), Constant(sub_dataset_),
                                      Constant(DataIngestionModule.get_gas_price_threshold(self._dataset,
                                                                                           address_,
                                                                                           sub_dataset_))])

    def get_thr_out_bal_atom(self, address_, sub_dataset_):
        return Atom('Thr_out_bal', [Constant(address_), Constant(sub_dataset_),
                                    Constant(DataIngestionModule.get_out_balance_threshold(self._dataset,
                                                                                           address_,
                                                                                           sub_dataset_))])

    def get_thr_in_bal_atom(self, address_, sub_dataset_):
        return Atom('Thr_in_bal', [Constant(address_), Constant(sub_dataset_),
                                   Constant(
                                       DataIngestionModule.get_in_balance_threshold(self._dataset, address_,
                                                                                    sub_dataset_))])

    def get_thr_out_degree_atom(self, address_, sub_dataset_):
        return Atom('Thr_out_degree', [Constant(address_), Constant(sub_dataset_),
                                       Constant(
                                           DataIngestionModule.get_out_degree_threshold(self._dataset,
                                                                                        address_,
                                                                                        sub_dataset_))])

    def get_thr_in_degree_atom(self, address_, sub_dataset_):
        return Atom('Thr_in_degree', [Constant(address_), Constant(sub_dataset_),
                                      Constant(
                                          DataIngestionModule.get_in_degree_threshold(self._dataset,
                                                                                      address_,
                                                                                      sub_dataset_))])

    def add_atom(self, atom):
        self._knowledge_base.add_ont_knowledge([atom])

    def get_attractiveness_atom(self, df, tx_sd, tx_from, tx_bn):
        start = self._dataset[self._dataset['sd'] == tx_sd]['blockNumber'].min()
        print('[attractiveness - sd: ' + str(tx_sd) + ' ] fromBlockNumber: ' + str(tx_bn) + ' window: ' + str(
            tx_bn - start))

        return Atom('Attractiveness', [Constant(tx_sd), Constant(tx_from), Constant(tx_bn),
                                       Constant(self.attractiveness(df, tx_from, tx_bn, tx_bn - start))])

    def get_in_degree_atom(self, df, tx_sd, tx_from, tx_bn):
        return Atom('InDegree', [Constant(tx_sd), Constant(tx_from), Constant(tx_bn),
                                 Constant(self.get_in_degree(df, tx_from, tx_bn))])

    def get_out_degree_atom(self, df, tx_sd, tx_from, tx_bn):
        return Atom('OutDegree', [Constant(tx_sd), Constant(tx_from), Constant(tx_bn),
                                  Constant(self.get_out_degree(df, tx_from, tx_bn))])

    def get_atoms(self):
        return self._atoms

    def get_out_degree(self, df, address, blockNumber):
        assert len(address) == 42, 'address provided is incorrect'
        return len(self.out_transaction_per_block(df, address, blockNumber))

    def get_in_degree(self, df, address, blockNumber):
        assert len(address) == 42, 'address provided is incorrect'
        return len(self.in_transaction_per_block(df, address, blockNumber))

    def out_transaction_per_block(self, df, address, blockNumber):
        assert len(address) == 42, 'address provided is incorrect'
        df_aux = self.out_transactions(df, address)
        return df_aux[df_aux['blockNumber'] == blockNumber]

    def in_transaction_per_block(self, df, address, blockNumber):
        assert len(address) == 42, 'address provided is incorrect'
        df_aux = self.in_transactions(df, address)
        return df_aux[df_aux['blockNumber'] == blockNumber]

    @staticmethod
    def get_sub_datasets(df, _range):
        df = df.reset_index(drop=True)
        df.loc[:, 'sd'] = df.index / _range  # It categorizes the whole dataset in sub datasets
        df.loc[:, 'sd'] = df['sd'].apply(np.floor)
        return df

    @staticmethod
    def in_transactions(df, address):
        assert len(address) == 42, 'address provided is incorrect'
        return df[df['to'] == address]

    @staticmethod
    def out_transactions(df, address):
        assert len(address) == 42, 'address provided is incorrect'
        return df[df['from'] == address]

    # Returns the attractiveness of the address
    def attractiveness(self, df, address, fromBlockNumber, window):
        assert fromBlockNumber >= 0, 'invalid fromBlockNumber'
        assert fromBlockNumber >= window, 'window is lower than fromBlockNumber'
        assert len(address) == 42, 'address provided is incorrect'
        neighbors_in_t = self.neighborhood(df, fromBlockNumber, address)
        print("==================================================================")
        print("neighbors_in_t: ")
        print(neighbors_in_t)
        print('SIZE: ' + str(neighbors_in_t.size))
        if neighbors_in_t.size == 0:
            return 0
        neighborhood_T_not_t = self.neighborhood_from_to(df, fromBlockNumber - window, fromBlockNumber, address)
        if neighborhood_T_not_t.size == 0:
            return 0
        print("neighborhood_T_not_t: ")
        print(neighborhood_T_not_t)
        print('SIZE: ' + str(neighborhood_T_not_t.size))
        neighborhood_T_with_t = self.neighborhood_from_to(df, fromBlockNumber - window, fromBlockNumber + 1, address)
        print("neighborhood_T_with_t: ")
        print(neighborhood_T_with_t)
        print('SIZE: ' + str(neighborhood_T_with_t.size))
        not_changed_neighbors = len(np.intersect1d(neighbors_in_t, neighborhood_T_not_t))
        print("not_changed_neighbors: ")
        print(not_changed_neighbors)
        print('SIZE: ' + str(neighborhood_T_with_t.size))
        print("==================================================================")
        return 1 - (not_changed_neighbors / neighborhood_T_with_t.size)

    def neighborhood(self, df, blockNumber, address):
        assert len(address) == 42, 'address provided is incorrect'
        df_aux = self.in_transaction_per_block(df, address, blockNumber)
        return df_aux["from"].unique()

    def neighborhood_from_to(self, df, fromBlockNumber, toBlockNumber, address):
        assert fromBlockNumber <= toBlockNumber, 'fromBlockNumber should be leq to block number'
        assert len(address) == 42, 'address provided is incorrect'
        df_in = self.in_transactions(df, address)
        df_in_range = df_in[(df_in['blockNumber'] >= fromBlockNumber) & (df_in['blockNumber'] < toBlockNumber)]
        return df_in_range["from"].unique()

    @staticmethod
    def get_sd_to_address(df, sd, address):
        return df[(df['sd'] == sd) & (df['to'] == address)]

    @staticmethod
    def get_sd_from_address(df, sd, address):
        return df[(df['sd'] == sd) & (df['from'] == address)]

    @staticmethod
    def get_gas_price_threshold(df, address, sd):
        df_sd_address = DataIngestionModule.get_sd_from_address(df, sd, address)
        bn_group = df_sd_address.groupby('blockNumber')
        return (bn_group['gasPrice'].max()).max() * 0.8

    @staticmethod
    def get_in_degree_threshold(df, address, sd):
        df_sd_address = DataIngestionModule.get_sd_to_address(df, sd, address)
        if len(df_sd_address) == 0:
            return 0
        bn_group = df_sd_address.groupby('blockNumber')
        return bn_group['to'].count().max() * 0.8

    @staticmethod
    def get_out_degree_threshold(df, address, sd):
        df_sd_address = DataIngestionModule.get_sd_from_address(df, sd, address)
        if len(df_sd_address) == 0:
            return 0
        bn_group = df_sd_address.groupby('blockNumber')
        return bn_group['from'].count().max() * 0.8

    @staticmethod
    def get_out_balance_threshold(df, address, sd):
        df_sd_address = DataIngestionModule.get_sd_from_address(df, sd, address)
        if len(df_sd_address) == 0:
            return 0
        bn_group = df_sd_address.groupby('blockNumber')
        return bn_group['value'].max().max() * 0.8

    @staticmethod
    def get_in_balance_threshold(df, address, sd):
        df_sd_address = DataIngestionModule.get_sd_to_address(df, sd, address)
        if len(df_sd_address) == 0:
            return 0
        bn_group = df_sd_address.groupby('blockNumber')
        return bn_group['value'].max().max() * 0.8
