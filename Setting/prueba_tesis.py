import functools
import hashlib
import itertools
import json
import operator
import os, sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from datetime import datetime
import csv
import portion
import random
import string
import subprocess
from Diffusion_Process.NetDiffNode import NetDiffNode
from Diffusion_Process.NetDiffEdge import NetDiffEdge
from Diffusion_Process.NetDiffFact import NetDiffFact
from Diffusion_Process.NLocalLabel import NLocalLabel
from Diffusion_Process.GlobalLabel import GlobalLabel
from Diffusion_Process.NetDiffLocalRule import NetDiffLocalRule
from Diffusion_Process.NetDiffGlobalRule import NetDiffGlobalRule
from Diffusion_Process.Average import Average
from Diffusion_Process.EnhancedTipping import EnhancedTipping
from Ontological.NetDERKB import NetDERKB
from Ontological.NetDERChase import NetDERChase
from Ontological.NetDERQuery import NetDERQuery
from Ontological.NetDERTGD import NetDERTGD
from Ontological.NetDEREGD import NetDEREGD
from Ontological.Atom import Atom
from Ontological.GRE import GRE
from Ontological.Distinct import Distinct
from Ontological.Equal import Equal
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.Homomorphism import Homomorphism
from Ontological.NetCompTarget import NetCompTarget
from Ontological.RDBHomomorphism import RDBHomomorphism
from itertools import chain
from functools import reduce



config_db_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "configdb_tesis.json"
schema_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "schema_tesis.xml"
entire_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to999999_NormalTransaction.csv"
entire_contract_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to999999_ContractInfo.csv"

tmax = 2

# ---------------------------------------------------------------------------

# "atoms" lo voy a utilizar luego para crear la BD ontologica


atom1 = Atom('news', [Variable('Content'), Variable('FN_level')])
atom2 = GRE(Variable('FN_level'), Constant(0.1))
ont_head1 = Atom('hyp_fakenews', [Variable('Content')])
tgd_counter = 0
sub_datasets = 250
TH_GAS_PR = 'TH_GAS_PR'
TH_BAL_OUT = 'TH_BAL_OUT'
TH_BAL_IN = 'TH_BAL_IN'
TH_DG_OUT = 'TH_DG_OU'
TH_DG_IN = 'TH_DG_IN'
GAS_PRICE = 'GP'
OUT_DEGREE = 'OUTDEG'
IN_DEGREE = 'INDEG'
IN_BALANCE = 'INBAL'
OUT_BALANCE = 'OUTBAL'
BLOCK_NUMBER = 'BN'
ADDRESS = 'ADDR'
SD = 'SD'

# # news(Content, FN_level) ^ FN_level > 0.1 -> hyp_fakenews(Content) : (trending(trending(covid19 doesn\'t exist), [0.3,1]))
# tgd1 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom1, atom2], ont_head=[ont_head1],
#                  global_cond=[(glabel, portion.closed(0, 1))])
global inicio_kb
global fin_kb
global inicio_q
global fin_q
global inicio_di
global fin_din


drop_account = "TRUNCATE TABLE `account`;"
drop_edge = "TRUNCATE TABLE `edge`;"
drop_hyp_malicious = "TRUNCATE TABLE `hyp_malicious`; "
drop_hyp_same_person = "TRUNCATE TABLE `hyp_same_person`; "
drop_invoke = "TRUNCATE TABLE `invoke`; "
drop_is_eoa = "TRUNCATE TABLE `is_eoa`; "
drop_is_owner = "TRUNCATE TABLE `is_owner`; "
drop_is_smart_contract = "TRUNCATE TABLE `is_smart_contract`; "
drop_mapping = "TRUNCATE TABLE `mapping`; "
drop_net_diff_fact = "TRUNCATE TABLE `net_diff_fact`; "
drop_node = "TRUNCATE TABLE `node`; "
drop_null_info = "TRUNCATE TABLE `null_info`;"

def drop_tables(con):
    con.execute(drop_account)
    con.execute(drop_edge)
    con.execute(drop_hyp_malicious)
    con.execute(drop_hyp_same_person)
    con.execute(drop_invoke)
    con.execute(drop_is_eoa)
    con.execute(drop_is_owner)
    con.execute(drop_is_smart_contract)
    con.execute(drop_mapping)
    con.execute(drop_net_diff_fact)
    con.execute(drop_node)
    con.execute(drop_null_info)


def main():
    df_0to499_NormalTransaction = pd.read_csv(entire_tx_dataset)
    df_0to499_contractInfo = pd.read_csv(entire_contract_dataset)
    df_0to499_contractInfo = df_0to499_contractInfo.drop(columns=['createdTimestamp', 'createdTransactionHash', 'creatorIsContract', 'createValue', 'creationCode', 'contractCode'])
    df_0to499_contractInfo.rename(columns={'createdBlockNumber': 'blockNumber'}, inplace=True)
    assert_column_name(df_0to499_NormalTransaction, 'gasPrice')
    assert_column_name(df_0to499_NormalTransaction, 'value')

    df_0to499_NormalTransaction['value'] = df_0to499_NormalTransaction['value'].astype(float)
    df_0to499_NormalTransaction = df_0to499_NormalTransaction.drop(columns=['timestamp'])
    df_0to499_NormalTransaction = df_0to499_NormalTransaction[df_0to499_NormalTransaction["isError"] == 'None']
    df_0to499_NormalTransaction = df_0to499_NormalTransaction[df_0to499_NormalTransaction['to'] != 'None']

    df_splitted = get_sub_datasets(df_0to499_NormalTransaction, sub_datasets)
    df_0to499_contractInfo = get_sub_contract_info_datasets(df_0to499_contractInfo, df_splitted)



#isOwner, is_eoa and is_smart_contract come from a different DB

    for sd in df_splitted['sd'].unique():
        print('sd: ' + str(sd))
        df_tx_batch = df_splitted[df_splitted['sd'] == sd]
        df_contract_batch = df_0to499_contractInfo[df_0to499_contractInfo['sd'] == sd]

        #   Account(Address)
        df_account = df_tx_batch.filter(['from']).drop_duplicates().reset_index(drop=True)
        df_account.columns = ['2_address']
        df_account['1_primary_key'] = df_account.apply(lambda row: hash_row('account', row), axis=1)

        #   is_smart_contract(Address)
        df_isContract = df_contract_batch.filter(['address']).drop_duplicates().reset_index(drop=True)
        df_isContract.columns = ['2_address']
        df_isContract['1_primary_key'] = df_isContract.apply(lambda row: hash_row('is_smart_contract', row), axis=1)

        #   Is_EOS(Address)
        df_isEOA = df_account.copy()
        keys = list(df_account.columns)
        i1 = df_isEOA.set_index(keys).index
        i2 = df_isContract.set_index(keys).index
        df_isEOA = df_isEOA[~i1.isin(i2)]

        #   invoke(Address, Address, BlockNumber)
        df_invoke = df_tx_batch[df_tx_batch['callingFunction'] != '0x'].filter(['from', 'to', 'blockNumber'])
        df_invoke.columns = ['2_address', '3_address', '4_block_number']
        df_invoke = df_invoke.drop_duplicates().reset_index(drop=True)
        df_invoke['1_primary_key'] = df_invoke.apply(lambda row: hash_row('invoke', row), axis=1)

        #   Is_owner(Address, Address,)
        df_is_owner = df_tx_batch[df_tx_batch['creates'] != 'None'][['from', 'creates']].reset_index(drop=True)
        df_contract_owner = df_contract_batch.filter(['address', 'creator'])
        df_contract_owner.columns = ['2_address', '3_address']
        df_is_owner.columns = ['2_address', '3_address']
        df_is_owner = df_is_owner.merge(df_contract_owner)
        df_is_owner = pd.concat([df_is_owner, df_contract_owner]).drop_duplicates().reset_index(drop=True)
        df_is_owner['1_primary_key'] = df_is_owner.apply(lambda row: hash_row('is_owner', row), axis=1)

        #   hyp_malicious
        df_malicious = df_tx_batch.filter(['from', 'blockNumber']).drop_duplicates().reset_index(drop=True)
        df_malicious.columns = ['2_address', '3_block_number']
        df_malicious['1_primary_key'] = df_malicious.apply(lambda row: hash_row('hyp_malicious', row), axis=1)

        #   Atoms, TGDs & EGDs
        atom_account_a1 = Atom('account', [Variable('A1')])
        atom_hyp_same_person = Atom('hyp_same_person', [Variable('A1'), Variable('A2')])


        atom_hyp_eoa_malicioso_a1 = Atom('hyp_malicious', [Variable('A1'), Variable('B')])
        atom_hyp_eoa_malicioso_a2 = Atom('hyp_malicious', [Variable('A2'), Variable('B1')])
        atom_is_eoa_a1 = Atom('is_eoa', [Variable('A1')])
        atom_is_owner_a1_c1 = Atom('is_owner', [Variable('A1'), Variable('C1')])
        atom_different_accounts_a1_a2 = Distinct(Variable('A1'), Variable('A2'))
        atom_gre_block_numbers = GRE(Variable('B1'), Variable('B'))
        atom_invoke_a2_c1 = Atom('invoke', [Variable('A2'), Variable('C1'), Variable('B1')])
        atom_invoke_a1_c1 = Atom('invoke', [Variable('A1'), Variable('C1'), Variable('B1')])

        atom_is_owner_a2_c1 = Atom('is_owner', [Variable('A2'), Variable('C1')])

        #R6.1 hyp_malicioso(A1, B) & is_EOA(A1) & es_owner(A1, C1) & A1 != A2 & invoca(A2, C1, B1) & (B < B1) → hyp_misma_persona(A1, A2)
        tgd_invoke_account_rule = NetDERTGD(rule_id=1, ont_body=[atom_hyp_eoa_malicioso_a1, atom_is_eoa_a1, atom_is_owner_a1_c1,
                                                         atom_invoke_a2_c1, atom_gre_block_numbers, atom_different_accounts_a1_a2], ont_head=[atom_hyp_same_person])

        #R6.3 hyp_malicioso(A1, B) & invoca(A1, C1, B1) & (B < B1) & es_owner(A2, C1) & (A1 != A2)  →  hyp_malicioso(A2, B1)
        tgd_invoke_malicious = NetDERTGD(rule_id=2, ont_body=[atom_hyp_eoa_malicioso_a1, atom_invoke_a1_c1, atom_is_owner_a2_c1, atom_gre_block_numbers,
                                                                 atom_different_accounts_a1_a2], ont_head=[atom_hyp_eoa_malicioso_a2])

        atom_hyp_sc_malicioso_c1 = Atom('hyp_malicious', [Variable('C1'), Variable('B')])
        atom_is_smart_contract_c1 = Atom('is_smart_contract', [Variable('C1')])
        atom_is_owner_a1_c2 = Atom('is_owner', [Variable('A1'), Variable('C2')])
        atom_invoke_a2_c2 = Atom('invoke', [Variable('A2'), Variable('C2'), Variable('B1')])
        atom_different_contracts_c1_c2 = Distinct(Variable('C1'), Variable('C2'))

        #R6.2 hyp_malicioso(C1, B) & es_SC(C1) & es_owner(A1, C1) & es_owner(A1, C2) & C1 != C2 & invoca(A2, C2, B1) & (B < B1) → hyp_misma_persona(A1, A2)
        tgd_invoke_contract_rule = NetDERTGD(rule_id=3,
                                            ont_body=[atom_hyp_sc_malicioso_c1, atom_is_smart_contract_c1, atom_is_owner_a1_c1, atom_is_owner_a1_c2 ,
                                                      atom_invoke_a2_c2, atom_different_contracts_c1_c2, atom_gre_block_numbers], ont_head=[atom_hyp_same_person])

        # E A2 hyp_misma_persona(A1,A2)
        ont_head_existential = [Atom('account', [Variable('A2')]), Atom('hyp_same_person', [Variable('A1'), Variable('A2')])]

        # R6.4 account(A1) & hyp_malicioso(A1,B) → E A2 hyp_misma_persona(A1,A2)
        tgd_exist_same_account = NetDERTGD(rule_id=4,
                                             ont_body=[atom_account_a1, atom_hyp_eoa_malicioso_a1],
                                             ont_head=[ont_head_existential])

        atom_invoke_a3_c2_b1 = Atom('invoke', [Variable('A3'), Variable('C2'), Variable('B1')])
        # hyp_misma_persona(A1,A2) & hyp_malicioso(C1, B) & es_owner(A1,C1) & es_owner(A1,C2) & (C1 != C2) & invoca(A3,C2, B1) & (B < B1) → A2 = A3

        egd1 = NetDEREGD(rule_id=5, ont_body=[atom_hyp_same_person, atom_hyp_sc_malicioso_c1, atom_is_owner_a1_c1, atom_is_owner_a1_c2, atom_invoke_a3_c2_b1, atom_different_contracts_c1_c2, atom_gre_block_numbers],
                         head=[Variable('A2'), Variable('A3')])

        egd2 = NetDEREGD(rule_id=6, ont_body=[atom_hyp_same_person, atom_hyp_sc_malicioso_c1, atom_is_owner_a1_c1,
                                              atom_is_owner_a1_c2, atom_invoke_a3_c2_b1, atom_different_contracts_c1_c2,
                                              atom_gre_block_numbers],
                         head=[Variable('A2'), Variable('A3')])

        kb = NetDERKB(data=set(), net_diff_graph=[], config_db=config_db_path, schema_path=schema_path,
                  netder_tgds=[tgd_invoke_account_rule, tgd_invoke_malicious, tgd_invoke_contract_rule, tgd_exist_same_account], netder_egds=[egd2], netdiff_lrules=[], netdiff_grules=[])

        kb.get_connection()

        cur = kb.get_connection().cursor()
        drop_tables(cur)

        engine = create_engine("mariadb+mariadbconnector://user:@127.0.0.1:3306/test_tesis")
        df_account.to_sql('account', con=engine, index=False, if_exists='append')
        df_invoke.to_sql('invoke', con=engine, index=False, if_exists='append')
        df_is_owner.to_sql('is_owner', con=engine, index=False, if_exists='append')
        df_isContract.to_sql('is_smart_contract', con=engine, index=False, if_exists='append')
        df_isEOA.to_sql('is_eoa', con=engine, index=False, if_exists='append')
        df_malicious.to_sql('hyp_malicious', con=engine, index=False, if_exists='append')


        # query =
        # _h = RDBHomomorphism(kb)
        # ts = _h.to_SQL(query)
        # print(ts)

        fin_kb = datetime.now()
        chase = NetDERChase(kb, tmax)
        # query1(HFN) = hyp_fakenews(HFN):[tmax, tmax], tener en cuenta que en este caso el tiempo no tiene importancia
        query1 = NetDERQuery(ont_cond=[atom_hyp_same_person], time=(tmax, tmax))
        inicio_q = datetime.now()
        answers = chase.answer_query(query1, 1)
        fin_q = datetime.now()
        print("NetDER Query")
        print(query1)
        print('-----')
        for ans in answers:
            for key in ans.keys():
                print("Variable", key, "instanciada con valor", ans[key].getValue())

        print('NetDERChase.contador', NetDERChase.contador)
        # print('NetDERKB.counter_graph', NetDERKB.counter_graph)

        inicio = datetime.now()

        # test1()
        # test2()

        fin = datetime.now()

        print('tiempo total:', (fin - inicio))
        print('tiempo de traduccion:', RDBHomomorphism.TRANSLATE_TIME)
        print('tiempo de construccion de homomorfismos', RDBHomomorphism.HOMOMORPH_BUILT_TIME)
        print('tiempo de ejecucion consulta SQL', RDBHomomorphism.HOMOMORPH_SQL_QUERY)
        # print('tiempo creacion kb:', (fin_kb - inicio_kb))
        print('tiempo para responder consulta:', (fin_q - inicio_q))

    # coso = str(id) + ',' + str(hash(_hash(_from))) + ',' + str(hash(_hash(_to))) + ','
    #     print(hash(_hash(coso)))
    #     hash(_hash(coso))

# def hash_row(df_name, row):
#     return hash(_hash(str(df_name) + ',' + str(hash(_hash(row['2_address']))) + ',' + str(hash(_hash(row['3_address']))) + ',' + str(hash(_hash(row['4_block_number']))) + ','))

def hash_row(df_name, row):
    _id = str(df_name) + ','
    for idx, val in row.items():
        _id = _id + str(hash(_hash(val))) + ','
    return hash(_hash(_id))

def _hash(elem):
    str_elem = str(elem)
    str_elem_encoded = str_elem.encode('utf-8')
    return hash(int(hashlib.sha1(str_elem_encoded).hexdigest(), 16))

def get_sub_contract_info_datasets(df_0to499_contractInfo, df_splitted):
    df_per_join = df_splitted.filter(['blockNumber', 'sd']).drop_duplicates()
    df_0to499_contractInfo = df_0to499_contractInfo.join(df_per_join.set_index('blockNumber'), on='blockNumber')
    return df_0to499_contractInfo


def get_sub_datasets(df, _range):
    # df_blockNumbers = df.filter(['blockNumber'])
    df['sd'] = df['blockNumber'] / _range
    df.loc[:, 'sd'] = df['sd'].apply(np.floor)
    # df.loc[:, 'sd'] = df.blockNumber / _range  # It categorizes the whole dataset in sub datasets
    # df.loc[:, 'sd'] = df['sd'].apply(np.floor)
    return df

def assert_column_name(df, name):
    assert name is not list(df.columns.values), str(name) + " is not in the columns)"

if __name__ == "__main__":
    main()
    exit(0)


#     print('------')
#     coso = str(id) + ',' + str(hash(_hash(_from))) + ',' + str(hash(_hash(_to))) + ','
#     print(hash(_hash(coso)))
#     print(coso)

#     print('------')
#
#
