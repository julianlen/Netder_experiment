import hashlib
import json
import os, sys

import numpy as np
import pandas as pd

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

config_db_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "config_db_tesis.json"
schema_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "schema_tesis.xml"
entire_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to499_NormalTransaction"

tmax = 2

# ---------------------------------------------------------------------------

# "atoms" lo voy a utilizar luego para crear la BD ontologica


atom1 = Atom('news', [Variable('Content'), Variable('FN_level')])
atom2 = GRE(Variable('FN_level'), Constant(0.1))
ont_head1 = Atom('hyp_fakenews', [Variable('Content')])
tgd_counter = 0
sub_datasets = 500
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


def main():
    df_0to499_NormalTransaction = pd.read_csv(entire_dataset)

    assert_column_name(df_0to499_NormalTransaction, 'gasPrice')
    assert_column_name(df_0to499_NormalTransaction, 'value')

    inicio_di = datetime.now()
    df_0to499_NormalTransaction['value'] = df_0to499_NormalTransaction['value'].astype(float)
    df_0to499_NormalTransaction = df_0to499_NormalTransaction.drop(columns=['timestamp'])
    df_0to499_NormalTransaction = df_0to499_NormalTransaction[df_0to499_NormalTransaction["isError"] == 'None']
    df_0to499_NormalTransaction = df_0to499_NormalTransaction[df_0to499_NormalTransaction['to'] != 'None']
    df_splitted = get_sub_datasets(df_0to499_NormalTransaction, sub_datasets)


#isOwner, is_eoa and is_smart_contract come from a different DB

    for sd in df_splitted['sd']:
        df_new_batch = df_splitted[df_splitted['sd'] == sd]
        df_account = df_new_batch['from'].reset_index(drop=True)
        df_account.columns = ['2_address']
        df_invoke = df_new_batch[df_new_batch['callingFunction'] != '0x']['from', 'to', 'blockNumber'].reset_index(drop=True)






    # dummy_graph = NetDiffGraph('graph', [], [])
    inicio_kb = datetime.now()
    kb = NetDERKB(data=set(), net_diff_graph=[], config_db=config_db_path, schema_path=schema_path,
                  netder_tgds=[], netdiff_lrules=[], netdiff_grules=[])
    # kb.add_ont_data(atoms)

    fin_kb = datetime.now()
    chase = NetDERChase(kb, tmax)
    # query1(HFN) = hyp_fakenews(HFN):[tmax, tmax], tener en cuenta que en este caso el tiempo no tiene importancia
    query1 = NetDERQuery(ont_cond=[Atom('hyp_fakenews', [Variable('HFN')])], time=(tmax, tmax))
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
    print('tiempo creacion kb:', (fin_kb - inicio_kb))
    print('tiempo para responder consulta:', (fin_q - inicio_q))


def get_sub_datasets(df, _range):
    df = df.reset_index(drop=True)
    df.loc[:, 'sd'] = df.index / _range  # It categorizes the whole dataset in sub datasets
    df.loc[:, 'sd'] = df['sd'].apply(np.floor)
    return df

def assert_column_name(df, name):
    assert name is not list(df.columns.values), str(name) + " is not in the columns)"

    def __hash__(self):
        string = str(self._id) + ','
        for term in self._terms:
            string = string + str(hash(term)) + ','

        string = string.encode('utf-8')
        return int(hashlib.sha1(string).hexdigest(), 16)

if __name__ == "__main__":
    main()
    exit(0)




#     print('------')
#     coso = str(id) + ',' + str(hash(_hash(_from))) + ',' + str(hash(_hash(_to))) + ','
#     print(coso)
#     print(hash(_hash(coso)))
#     print('------')
#
#
# def _hash(elem):
#     str_elem = str(elem)
#     str_elem_encoded = str_elem.encode('utf-8')
#     return int(hashlib.sha1(str_elem_encoded).hexdigest(), 16)