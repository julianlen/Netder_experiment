import hashlib
import os, sys
import mariadb as maria

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from Setting.EvaluatorTesis import EvaluatorTesis

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from datetime import datetime
from Ontological.NetDERKB import NetDERKB
from Ontological.NetDERChase import NetDERChase
from Ontological.NetDERQuery import NetDERQuery
from Ontological.NetDERTGD import NetDERTGD
from Ontological.NetDEREGD import NetDEREGD
from Ontological.Atom import Atom
from Ontological.GRE import GRE
from Ontological.Distinct import Distinct
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.RDBHomomorphism import RDBHomomorphism



config_db_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "configdb_tesis.json"
schema_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "schema_tesis.xml"
entire_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to999999_NormalTransaction.csv"
entire_contract_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to999999_ContractInfo.csv"
dummy_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_transactions"
dummy_contract_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_contracts"
dummy_ground_truth = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_ground_truth"

tmax = 2

# ---------------------------------------------------------------------------

# "atoms" lo voy a utilizar luego para crear la BD ontologica


atom1 = Atom('news', [Variable('Content'), Variable('FN_level')])
atom2 = GRE(Variable('FN_level'), Constant(0.1))
ont_head1 = Atom('hyp_fakenews', [Variable('Content')])
tgd_counter = 0
sub_datasets = 1
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

global inicio_kb
global fin_kb
global inicio_q
global fin_q
global inicio_di
global fin_din

def main():
    #   --------------------------
    #   GETTING & MERGING DATASETS
    #   --------------------------

    df_transactionInfo = pd.read_csv(dummy_tx_dataset)
    df_contractInfo = pd.read_csv(dummy_contract_dataset)

    df_contractInfo = df_contractInfo.drop(columns=['createdTimestamp','creatorIsContract','creationCode','contractCode','createdTransactionHash']).rename(columns={'createdBlockNumber':'blockNumber', 'address':'creates', 'creator':'from', 'createValue':'value'})
    df_contractInfo['to'] = 'None'
    df_contractInfo['gasPrice'] = 500000000000
    df_contractInfo['callingFunction'] = '0x'
    df_contractInfo['value'] = df_contractInfo['value'].astype(float)

    df_contractInfo.rename(columns={'createdBlockNumber': 'blockNumber', 'address':'creates', 'creator':'from', 'createValue':'value'}, inplace=True)

    df_transactionInfo['value'] = df_transactionInfo['value'].astype(float)
    df_transactionInfo = df_transactionInfo.drop(columns=['timestamp', 'transactionHash', 'gasUsed', 'gasLimit'])
    df_transactionInfo = df_transactionInfo[df_transactionInfo["isError"] == 'None'].drop(columns=['isError']).drop_duplicates().reset_index(drop=True)
    df_transactionInfo = pd.concat([df_transactionInfo, df_contractInfo]).drop_duplicates().reset_index(drop=True)
    df_transactionInfo = df_transactionInfo.sort_values(by='blockNumber')


    evaluator = EvaluatorTesis(pd.read_csv(dummy_ground_truth))

    #   --------------------------
    #   DATA DIGESTION
    #   --------------------------

    # Split in sub datasets
    df_transactionInfo_splitted = get_sub_datasets(df_transactionInfo, sub_datasets)

    # # CONTRACT CREATION
    df_contracts_created = df_transactionInfo.filter(['blockNumber', 'from', 'creates'])
    df_contracts_created = df_contracts_created[(df_contracts_created['creates'] != 'None')]
    df_contracts_created = df_contracts_created.groupby(['blockNumber', 'from']).count().groupby(
        level=['from']).cumsum().reset_index()
    df_contracts_created['threshold'] = df_contracts_created['creates'] * 0.5
    df_contracts_created = df_contracts_created.rename(columns={'from': 'owner'})
    df_contracts_created_atoms = df_contracts_created.filter(['blockNumber', 'owner', 'creates'])
    df_contracts_created_atoms = get_sub_datasets(df_contracts_created_atoms, sub_datasets)
    df_contracts_created_atoms.rename(columns={'sd': '2_sd', 'blockNumber':'3_blockNumber','owner': '4_address', 'creates': '5_contracts_created'},
                             inplace=True)
    df_contracts_created_atoms['1_primary_key'] = df_contracts_created_atoms.apply(
        lambda row: hash_row_with_sd('contracts_created', row), axis=1).drop_duplicates().reset_index(drop=True)

    # INVOCACIONES
    df_creaciones = df_transactionInfo[(df_transactionInfo['creates'] != 'None')].filter(
        ['from', 'creates']).reset_index(drop=True)  # Me quedo con los contratos creados y su creador
    df_invocaciones = df_transactionInfo[(df_transactionInfo['callingFunction'] != '0x')].filter(
        ['sd', 'blockNumber', 'to'])  # Me quedo con las invocaciones y a quién se dirige.
    df_invocaciones = df_invocaciones.loc[df_invocaciones['to'].isin(df_creaciones['creates'])].reset_index(
        drop=True)  # Filtro las invocaciones dentro de los contratos creados
    df_creaciones = df_creaciones.set_index(df_creaciones['creates'], drop=True).drop(columns=['creates']).rename(
        columns={'from': 'owner'})  # Setteo el address del contrato como indice
    df_invocaciones_aux = df_invocaciones.join(df_creaciones, on='to').drop(columns=[
        'to'])  # Hago join entre el address del contrato y el contrato invocado, me quedarían todas las veces que se invoco a cada contrato, y el owner
    df_invocaciones_aux['#invocations'] = 0
    df_invocaciones = df_invocaciones_aux.groupby(['blockNumber', 'owner']).count().groupby(level=['owner'],
                                                                                                  sort=False).cumsum().reset_index()  # Esto me va a generr repetidos, que nos sirve ya que es la cantidad de veces que se llamo al contrto
    df_invocaciones_atom = get_sub_datasets(df_invocaciones, sub_datasets)
    df_invocaciones_atom.rename(columns={'sd': '2_sd', 'blockNumber':'3_blockNumber','owner': '4_address', '#invocations': '5_invocaciones'},
                             inplace=True)
    df_invocaciones_atom['1_primary_key'] = df_invocaciones_atom.apply(
        lambda row: hash_row_with_sd('invocaciones', row), axis=1).drop_duplicates().reset_index(drop=True)


    # TRANSFERENCIAS
    # Me quedo con los creates, ya que nos interesan los dueños de los contratos a los que tenemos acceso y no los contratos.
    df_creaciones = df_transactionInfo[(df_transactionInfo['creates'] != 'None')].filter(
        ['from', 'creates']).reset_index(drop=True)
    df_transferencias = df_transactionInfo.filter(['blockNumber', 'to', 'value']).reset_index(
        drop=True)  # Me quedo con las transferencias de valor
    df_transferencias = df_transferencias.loc[df_transferencias['to'].isin(df_creaciones['creates'])].reset_index(
        drop=True)  # Me quedo con las transf. a contratos creados.
    df_creaciones = df_creaciones.set_index(df_creaciones['creates'], drop=True).drop(columns=['creates']).rename(
        columns={'from': 'owner'})  # Filtro las transf. dentro de los contratos creados
    df_transferencias_aux = df_transferencias.join(df_creaciones,
                                                   on='to')  # Hago join entre el address del contrato y el contrato transferido, me queda todas las transferencias a cada contrato y su owner.
    df_transferencias = df_transferencias_aux.groupby(['blockNumber', 'owner']).sum().groupby(level=['owner'],
        sort=False).cumsum().reset_index()  # Cuento cuanto queda de valor

    df_transferencias_atom = get_sub_datasets(df_transferencias, sub_datasets)
    df_transferencias_atom.rename(columns={'sd': '2_sd', 'blockNumber':'3_blockNumber','owner': '4_address', 'value': '5_value_transferido'},
                             inplace=True)
    df_transferencias_atom['1_primary_key'] = df_transferencias_atom.apply(
        lambda row: hash_row_with_sd('transferencias', row), axis=1).drop_duplicates().reset_index(drop=True)


    # Calculo de threshold para invocaciones
    # El siguiente codigo realiza lo siguiente. El threshold de invocaciones/transferencias depende de la cantidad de contratos creados por una cuenta
    # Para esto primero se calcula los contratos creados por una cuenta. Lo que se hace es, cada vez que la cuenta X crea un contrato en el bloque BN,
    # se crea un atomo que la cuenta X en el bloque BN creo 1 contrato. Luego si en el BN5 X crea un nuevo contrato, se cuenta que en el BN2
    # X creo 2 contratos. Ahora, el threshold T1 de las invocaciones es en función a los contratos creados. Esta función es T1 = contratos_creados*0.5.
    # Pero dado que el algoritmo original tomaba una foto estática de la blockchain y en este caso es dinámica (ingresan de a subdatasets), por cada subdataset nuevo,
    # en particular por cada bloque nuevo, se re computa el threshold. Entonces si en el BN1 hay una invocacion a un contrato de X, se
    # busca el último BN donde se computo lso contratos creados por X, se lo multiplica por 0.5 devolviendo T1 y se chequea que la cantidad de invocaciones
    # sea mayor a T1. Ahora si se invoca a un contrato creado por X en el bloque BN8, la cantidad de contratos creados por X fue computada en BN5. O sea 2.
    # Es importante remarcar que los thresholds en este caso se hacen por block number y no por sd.

    df_threshold_invocaciones = pd.merge(df_contracts_created, df_invocaciones, how="inner", on=["owner"])
    df_threshold_invocaciones = df_threshold_invocaciones[
        df_threshold_invocaciones['blockNumber_x'] <= df_threshold_invocaciones['blockNumber_y']]
    df_threshold_invocaciones = df_threshold_invocaciones.groupby(
        ['sd_y', 'blockNumber_y', 'owner']).max().reset_index().filter(
        ['sd_y', 'blockNumber_y', 'owner', 'blockNumber_x', 'threshold', '#invocations'])
    df_threshold_invocaciones['threshold'] = df_threshold_invocaciones['threshold'] * df_threshold_invocaciones[
        '#invocations']
    df_threshold_invocaciones = df_threshold_invocaciones.filter(
        ['sd_y', 'blockNumber_y', 'owner', 'threshold', '#invocations']).rename(
        columns={'sd_y': 'sd', 'blockNumber_y': 'blockNumber'})
    #
    # # Calculo de threshold para transferencias
    # df_threshold_transferencias = pd.merge(df_contracts_created, df_transferencias, how="inner", on=["owner"])
    # df_threshold_transferencias = df_threshold_transferencias[
    #     df_threshold_transferencias['blockNumber_x'] <= df_threshold_transferencias['blockNumber_y']]
    # df_threshold_transferencias = df_threshold_transferencias.groupby(
    #     ['sd_y', 'blockNumber_y', 'owner']).max().reset_index().filter(
    #     ['sd_y', 'blockNumber_y', 'owner', 'blockNumber_x', 'threshold', 'value'])
    # df_threshold_transferencias['threshold'] = df_threshold_transferencias['threshold'] * df_threshold_transferencias[
    #     'value']
    # df_threshold_transferencias = df_threshold_transferencias.filter(
    #     ['sd_y', 'blockNumber_y', 'owner', 'threshold', 'value']).rename(
    #     columns={'sd_y': 'sd', 'blockNumber_y': 'blockNumber'})



    # Get threshold per dataset per account

    max_degree = df_transactionInfo_splitted.filter(['sd','blockNumber','from','to']).drop_duplicates().reset_index(drop=True)

    max_degree_in = max_degree.groupby(['sd', 'blockNumber', 'to']).count().reset_index().filter(['sd', 'from', 'to'])
    df_thr_degree_in = (max_degree_in.groupby(['sd', 'to']).max() * 0.8).reset_index()
    df_thr_degree_in.rename(columns={'sd': '2_sd', 'to': '3_address', 'from': '4_thr_degree_in'},
                             inplace=True)
    df_thr_degree_in['1_primary_key'] = df_thr_degree_in.apply(
        lambda row: hash_row_with_sd('threshold_degree_in', row), axis=1).drop_duplicates().reset_index(drop=True)

    max_degree_out = max_degree.groupby(['sd', 'blockNumber', 'from']).count().reset_index().filter(['sd', 'from', 'to'])
    df_thr_degree_out = (max_degree_out.groupby(['sd', 'from']).max() * 0.8).reset_index()
    df_thr_degree_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'to': '4_thr_degree_out'},
                               inplace=True)
    df_thr_degree_out['1_primary_key'] = df_thr_degree_out.apply(
        lambda row: hash_row_with_sd('threshold_degree_out', row), axis=1).drop_duplicates().reset_index(drop=True)


    max_gas_price_out = df_transactionInfo_splitted.filter(['sd','from','gasPrice']).drop_duplicates().reset_index(drop=True)
    grouped_max_gas_price_out = max_gas_price_out.groupby(['sd', 'from'])
    df_thr_gasPrice_out = grouped_max_gas_price_out.apply(lambda x: x.max() * 0.8).reset_index()
    df_thr_gasPrice_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'gasPrice': '4_thr_gasPrice_out'},
                              inplace=True)
    df_thr_gasPrice_out['1_primary_key'] = df_thr_gasPrice_out.apply(
        lambda row: hash_row_with_sd('threshold_gasPrice_out', row), axis=1).drop_duplicates().reset_index(drop=True)

    max_gas_price_in = df_transactionInfo_splitted.filter(['sd', 'to', 'gasPrice']).drop_duplicates().reset_index(drop=True)
    grouped_max_gas_price_in = max_gas_price_in.groupby(['sd', 'to'])
    df_thr_gasPrice_in = grouped_max_gas_price_in.apply(lambda x: x.max() * 0.8).reset_index()
    df_thr_gasPrice_in.rename(columns={'sd': '2_sd', 'to': '3_address', 'gasPrice': '4_thr_gasPrice_in'}, inplace=True)
    df_thr_gasPrice_in['1_primary_key'] = df_thr_gasPrice_in.apply(
        lambda row: hash_row_with_sd('threshold_gasPrice_in', row), axis=1).drop_duplicates().reset_index(drop=True)

    max_balance_out = df_transactionInfo_splitted.filter(['sd', 'from', 'value']).drop_duplicates().reset_index(drop=True)
    grouped_max_balance_out = max_balance_out.groupby(['sd', 'from'])
    df_thr_balance_out = grouped_max_balance_out.apply(lambda x: x.max() * 0.8).reset_index()
    df_thr_balance_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'value': '4_thr_balance_out'}, inplace=True)
    df_thr_balance_out['1_primary_key'] = df_thr_balance_out.apply(
        lambda row: hash_row_with_sd('threshold_balance_out', row), axis=1).drop_duplicates().reset_index(drop=True)

    max_balance_in = df_transactionInfo_splitted.filter(['sd', 'to', 'value']).drop_duplicates().reset_index(drop=True)
    grouped_max_balance_in = max_balance_in.groupby(['sd', 'to'])
    df_thr_balance_in = grouped_max_balance_in.apply(lambda x: x.max() * 0.8).reset_index()
    df_thr_balance_in.rename(columns={'sd':'2_sd', 'to':'3_address', 'value':'4_thr_balance_in'}, inplace=True)
    df_thr_balance_in['1_primary_key'] = df_thr_balance_in.apply(lambda row: hash_row_with_sd('threshold_balance_in', row), axis=1).drop_duplicates().reset_index(drop=True)

    # TODO: Renombrar columnas threshold


    # Gets contract creations
    df_contract_creation = df_transactionInfo_splitted[df_transactionInfo_splitted['creates'] != 'None']

    # Gets created contract addresses for is_contract
    df_contract_creation_addresses = df_contract_creation.filter(['creates', 'sd']).drop_duplicates().reset_index(drop=True)
    df_contract_creation_addresses.rename(columns={'creates': '2_address'}, inplace=True)

    # Gets all accounts
    df_account_from = df_transactionInfo_splitted.filter(['from', 'sd'])
    df_account_from.rename(columns={'from': '2_address'}, inplace=True)
    df_account_to = df_transactionInfo_splitted.filter(['to', 'sd'])
    df_account_to.rename(columns={'to': '2_address'}, inplace=True)
    df_account = pd.concat([df_account_from, df_account_to, df_contract_creation_addresses], ignore_index=True).drop_duplicates().reset_index(drop=True)
    df_account.rename(columns={'from': '2_address'}, inplace=True)
    df_account['1_primary_key'] = df_account.apply(lambda row: hash_row('account', row), axis=1)

    # Gets invocations
    df_invoke = df_transactionInfo_splitted[df_transactionInfo_splitted['callingFunction'] != '0x'].filter(['from', 'to', 'blockNumber', 'sd']).drop_duplicates().reset_index(drop=True)
    df_invoke.columns = ['2_address', '3_address', '4_block_number', 'sd']
    df_invoke['1_primary_key'] = df_invoke.apply(lambda row: hash_row('invoke', row), axis=1)

    # Gets all contracts
    df_is_contract_by_invocation = df_transactionInfo_splitted[df_transactionInfo_splitted['callingFunction'] != '0x'].filter(['to', 'sd'])
    df_is_contract_by_invocation.rename(columns={'to': '2_address'}, inplace=True)
    df_isContract = pd.concat([df_contract_creation_addresses, df_is_contract_by_invocation], ignore_index=True).sort_values(by='sd').drop_duplicates(subset='2_address', keep='first').reset_index(drop=True)
    df_isContract['1_primary_key'] = df_isContract.apply(lambda row: hash_row('is_smart_contract', row), axis=1)

    # Gets all EOA
    df_isEOA = df_account.loc[~df_account['2_address'].isin(df_isContract['2_address'])].drop_duplicates(subset='2_address', keep='first').reset_index(drop=True)

    # Gets the owner of each contracts
    df_is_owner = df_contract_creation.filter(['from', 'creates', 'sd']).reset_index(drop=True)
    df_is_owner.columns = ['2_address', '3_address', 'sd']
    df_is_owner = df_is_owner.drop_duplicates().reset_index(drop=True)
    df_is_owner['1_primary_key'] = df_is_owner.apply(lambda row: hash_row('is_owner', row), axis=1)


    # Dummy ground truth
    df_malicious_data = {
        '2_address': ['em1', 'cm3'],
        '3_block_number': [1, 1],
        'sd': [1.0, 1.0],
    }
    df_malicious = pd.DataFrame(df_malicious_data, columns=['2_address', '3_block_number', 'sd'])
    df_malicious['1_primary_key'] = df_malicious.apply(lambda row: hash_row('hyp_malicious', row), axis=1)

    #   --------------------------
    #   Atoms, TGDs & EGDs
    #   --------------------------
    atom_account_a1 = Atom('account', [Variable('A1')])
    atom_hyp_same_person_b1 = Atom('hyp_same_person', [Variable('A1'), Variable('A2'), Variable('B1')])
    atom_hyp_same_person_b2 = Atom('hyp_same_person', [Variable('A1'), Variable('A2'), Variable('B2')])

    atom_hyp_eoa_malicioso_a1 = Atom('hyp_malicious', [Variable('A1'), Variable('B')])
    atom_hyp_eoa_malicioso_a2_b1 = Atom('hyp_malicious', [Variable('A2'), Variable('B1')])
    atom_hyp_eoa_malicioso_a2_b = Atom('hyp_malicious', [Variable('A2'), Variable('B')])
    atom_is_eoa_a1 = Atom('is_eoa', [Variable('A1')])
    atom_is_owner_a1_c1 = Atom('is_owner', [Variable('A1'), Variable('C1')])
    atom_different_accounts_a1_a2 = Distinct(Variable('A1'), Variable('A2'))
    atom_gre_block_numbers_b1_b = GRE(Variable('B1'), Variable('B'))
    atom_gre_block_numbers_b_b1 = GRE(Variable('B'), Variable('B1'))
    atom_gre_block_numbers_b1_b2 = GRE(Variable('B1'), Variable('B2'))
    atom_invoke_a2_c1 = Atom('invoke', [Variable('A2'), Variable('C1'), Variable('B1')])
    atom_invoke_a1_c1 = Atom('invoke', [Variable('A1'), Variable('C1'), Variable('B1')])

    atom_is_owner_a2_c1 = Atom('is_owner', [Variable('A2'), Variable('C1')])

    # R6.1 hyp_malicioso(A1, B) & is_EOA(A1) & es_owner(A1, C1) & A1 != A2 & invoca(A2, C1, B1) & (B < B1) → hyp_misma_persona(A1, A2, B1)
    tgd_invoke_account_rule = NetDERTGD(rule_id=1,
                                        ont_body=[atom_hyp_eoa_malicioso_a1, atom_is_eoa_a1, atom_is_owner_a1_c1,
                                                  atom_invoke_a2_c1, atom_gre_block_numbers_b1_b,
                                                  atom_different_accounts_a1_a2], ont_head=[atom_hyp_same_person_b1])

    # R6.3 hyp_malicioso(A1, B) & invoca(A1, C1, B1) & (B < B1) & es_owner(A2, C1) & (A1 != A2)  →  hyp_malicioso(A2, B1)
    tgd_invoke_malicious_rule = NetDERTGD(rule_id=2,
                                     ont_body=[atom_hyp_eoa_malicioso_a1, atom_invoke_a1_c1, atom_is_owner_a2_c1,
                                               atom_gre_block_numbers_b1_b,
                                               atom_different_accounts_a1_a2], ont_head=[atom_hyp_eoa_malicioso_a2_b1])

    atom_hyp_sc_malicioso_c1 = Atom('hyp_malicious', [Variable('C1'), Variable('B')])
    atom_hyp_malicioso_a1 = Atom('hyp_malicious', [Variable('A1'), Variable('B')])
    atom_is_owner_a1_c2 = Atom('is_owner', [Variable('A1'), Variable('C2')])
    atom_invoke_a2_c2 = Atom('invoke', [Variable('A2'), Variable('C2'), Variable('B1')])
    atom_different_contracts_c1_c2 = Distinct(Variable('C1'), Variable('C2'))

    # R6.2 hyp_malicioso(C1, B) & es_owner(A1, C1) & es_owner(A1, C2) & C1 != C2 & invoca(A2, C2, B1) & (B < B1) → hyp_misma_persona(A1, A2, B1)
    tgd_invoke_contract_rule = NetDERTGD(rule_id=3,
                                         ont_body=[atom_hyp_sc_malicioso_c1,
                                                   atom_is_owner_a1_c1, atom_is_owner_a1_c2,
                                                   atom_invoke_a2_c2, atom_different_contracts_c1_c2,
                                                   atom_gre_block_numbers_b1_b], ont_head=[atom_hyp_same_person_b1])
    # TODO: REVISAR ESTO
    # E A2 hyp_misma_persona(A1,A2)
    ont_head_existential = [Atom('account', [Variable('A2')]),
                            Atom('hyp_same_person', [Variable('A1'), Variable('A2'), Variable('B')])]

    # R6.4 hyp_malicioso(A1,B) → E A2 hyp_misma_persona(A1,A2, B)
    tgd_exist_same_account = NetDERTGD(rule_id=4,
                                       ont_body=[atom_account_a1, atom_hyp_eoa_malicioso_a1],
                                       ont_head=ont_head_existential)

    atom_invoke_a3_c2_b1 = Atom('invoke', [Variable('A3'), Variable('C2'), Variable('B1')])
    atom_invoke_a3_c1_b1 = Atom('invoke', [Variable('A3'), Variable('C1'), Variable('B1')])

    # hyp_misma_persona(A1,A2, B2) & hyp_malicioso(C1, B) & es_owner(A1,C1) & es_owner(A1,C2) & invoca(A3,C2, B1) & (C1 != C2) & (B2 < B1) & (B < B1) → A2 = A3
    egd1 = NetDEREGD(rule_id=5,
                     ont_body=[atom_hyp_same_person_b2, atom_hyp_sc_malicioso_c1, atom_is_owner_a1_c1, atom_is_owner_a1_c2,
                               atom_invoke_a3_c2_b1, atom_different_contracts_c1_c2, atom_gre_block_numbers_b1_b2, atom_gre_block_numbers_b1_b],
                     head=[Variable('A2'), Variable('A3')])

    # hyp_misma_persona(A1,A2,B2) & hyp_malicioso(A1, B) & invoca(A3,C1, B1) & es_owner(A1,C1) & (B2 < B1) & (B < B1) → A2 = A3
    egd2 = NetDEREGD(rule_id=7, ont_body=[atom_hyp_same_person_b2, atom_hyp_malicioso_a1, atom_invoke_a3_c1_b1, atom_is_owner_a1_c1,
                                          atom_different_contracts_c1_c2,
                                          atom_gre_block_numbers_b1_b, atom_gre_block_numbers_b1_b2],
                     head=[Variable('A2'), Variable('A3')])

    # hyp_misma_persona(A1,A2, B2) & invoca(A3,C1, B1) & es_owner(A1,C1) & (B2 < B1) → A2 = A3
    egd3 = NetDEREGD(rule_id=6, ont_body=[atom_hyp_same_person_b2, atom_invoke_a3_c1_b1,
                                          atom_is_owner_a1_c1, atom_gre_block_numbers_b1_b2],
                     head=[Variable('A2'), Variable('A3')])

    # hyp_malicious(A1,B) & hyp_same_person(A2,B1) & B1 > B -> hyp_malicious(A2, B1)
    tgd_same_person_malicious_v1 = NetDERTGD(rule_id=8, ont_body=[atom_hyp_malicioso_a1, atom_hyp_same_person_b1, atom_gre_block_numbers_b1_b],
                     ont_head=[atom_hyp_eoa_malicioso_a2_b1])

    # hyp_malicious(A1,B) & hyp_same_person(A2,B1) & B > B1 -> hyp_malicious(A2, B)
    tgd_same_person_malicious_v2 = NetDERTGD(rule_id=8, ont_body=[atom_hyp_malicioso_a1, atom_hyp_same_person_b1, atom_gre_block_numbers_b_b1],
                                          ont_head=[atom_hyp_eoa_malicioso_a2_b])

    # kb = NetDERKB(data=set(), net_diff_graph=[], config_db=config_db_path, schema_path=schema_path,
    #               netder_tgds=[tgd_invoke_account_rule, tgd_invoke_malicious, tgd_invoke_contract_rule,
    #                            tgd_exist_same_account], netder_egds=[], netdiff_lrules=[], netdiff_grules=[])
    kb = NetDERKB(data=set(), net_diff_graph=[], config_db=config_db_path, schema_path=schema_path,
                  netder_tgds=[tgd_invoke_account_rule, tgd_invoke_contract_rule, tgd_same_person_malicious_v1, tgd_same_person_malicious_v2], netder_egds=[], netdiff_lrules=[], netdiff_grules=[])

    cur = kb.get_connection().cursor()
    kb.get_connection().commit()

    for sd in df_transactionInfo_splitted['sd'].unique():
        print('\nsd: ' + str(sd))

        #   Account(Address)
        df_account_sd = df_account[df_account['sd'] == sd].filter(['1_primary_key', '2_address'])

        # contracts_created(SD, blockNumber, address, contracts_Created)
        df_contracts_created_atoms_sd = df_contracts_created_atoms[df_contracts_created_atoms['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_contracts_created'])

        # invocaciones(SD, blockNumber, address, invocaciones)
        df_invocaciones_atom_sd = df_invocaciones_atom[df_invocaciones_atom['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_invocaciones'])

        # transferencias(SD, blockNumber, address, value_transferido)
        df_transferencias_atom_sd = df_transferencias_atom[df_transferencias_atom['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_value_transferido'])

        #   threshold_degree_out(SD, Address, Degree_out)
        df_thr_degree_out_sd = df_thr_degree_out[df_thr_degree_out['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_address', '4_thr_degree_out'])

        #   threshold_degree_in(SD, Address, Degree_in)
        df_thr_degree_in_sd = df_thr_degree_in[df_thr_degree_in['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_address', '4_thr_degree_in'])

        #   threshold_gasPrice_out(SD, Address, GasPrice_out)
        df_thr_gasPrice_out_sd = df_thr_gasPrice_out[df_thr_balance_out['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_address', '4_thr_gasPrice_out'])

        #   threshold_gasPrice_in(SD, Address, GasPrice_in)
        df_thr_gasPrice_in_sd = df_thr_gasPrice_in[df_thr_balance_in['2_sd'] == sd].filter(
            ['1_primary_key', '2_sd', '3_address', '4_thr_gasPrice_in'])

        #   threshold_balance_in(SD, Address, Balance_In)
        df_thr_balance_in_sd = df_thr_balance_in[df_thr_balance_in['2_sd'] == sd].filter(['1_primary_key', '2_sd','3_address', '4_thr_balance_in'])

        #   threshold_balance_in(SD, Address, Balance_Out)
        df_thr_balance_out_sd = df_thr_balance_out[df_thr_balance_out['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_address', '4_thr_balance_out'])

        #   is_smart_contract(Address)
        df_isContract_sd = df_isContract[df_isContract['sd'] == sd].filter(['1_primary_key', '2_address'])

        #   Is_EOA(Address)
        df_isEOA_sd = df_isEOA[df_isEOA['sd'] == sd].filter(['1_primary_key', '2_address'])

        #   invoke(Address, Address, BlockNumber)
        df_invoke_sd = df_invoke[df_invoke['sd'] == sd].filter(['1_primary_key', '2_address', '3_address', '4_block_number'])

        #   Is_owner(Address, Address,)
        df_is_owner_sd = df_is_owner[df_is_owner['sd'] == sd].filter(['1_primary_key', '2_address', '3_address'])

        #   hyp_malicious
        df_malicious_sd = df_malicious[df_malicious['sd'] == sd].filter(['1_primary_key', '2_address', '3_block_number'])

        engine = create_engine("mariadb+mariadbconnector://user:@127.0.0.1:3306/test_tesis")

        df_transferencias_atom_sd.to_sql('transferencias', con=engine, index=False, if_exists='append')
        df_invocaciones_atom_sd.to_sql('invocaciones', con=engine, index=False, if_exists='append')
        df_contracts_created_atoms_sd.to_sql('contracts_created', con=engine, index=False, if_exists='append')
        df_thr_degree_out_sd.to_sql('threshold_degree_out', con=engine, index=False, if_exists='append')
        df_thr_degree_in_sd.to_sql('threshold_degree_in', con=engine, index=False, if_exists='append')
        df_thr_gasPrice_out_sd.to_sql('threshold_gasPrice_out', con=engine, index=False, if_exists='append')
        df_thr_gasPrice_in_sd.to_sql('threshold_gasPrice_in', con=engine, index=False, if_exists='append')
        df_thr_balance_out_sd.to_sql('threshold_balance_out', con=engine, index=False, if_exists='append')
        df_thr_balance_in_sd.to_sql('threshold_balance_in', con=engine, index=False, if_exists='append')
        df_invoke_sd.to_sql('invoke', con=engine, index=False, if_exists='append')
        df_is_owner_sd.to_sql('is_owner', con=engine, index=False, if_exists='append')
        df_isContract_sd.to_sql('is_smart_contract', con=engine, index=False, if_exists='append')
        df_isEOA_sd.to_sql('is_eoa', con=engine, index=False, if_exists='append')
        df_malicious_sd.to_sql('hyp_malicious', con=engine, index=False, if_exists='append')


        chase = NetDERChase(kb, tmax)

        query1 = NetDERQuery(ont_cond=[atom_hyp_same_person_b1], time=(tmax, tmax))
        query2 = NetDERQuery(ont_cond=[atom_hyp_malicioso_a1], time=(tmax, tmax))
        actual_query = query2
        inicio_q = datetime.now()
        answers = chase.answer_query(actual_query, 1)
        fin_q = datetime.now()
        print("NetDER Query")
        print(actual_query)
        print('-----')
        for ans in answers:
            for key in ans.keys():
                print("Variable", key, "instanciada con valor", ans[key].getValue())

        print(evaluator.evaluate(answers, df_account_sd, sd))

        print('NetDERChase.contador', NetDERChase.contador)
        print('tiempo de traduccion:', RDBHomomorphism.TRANSLATE_TIME)
        print('tiempo de construccion de homomorfismos', RDBHomomorphism.HOMOMORPH_BUILT_TIME)
        print('tiempo de ejecucion consulta SQL', RDBHomomorphism.HOMOMORPH_SQL_QUERY)
        print('tiempo para responder consulta:', (fin_q - inicio_q))

    cur.close()
    kb.get_connection().close()


def hash_row_with_sd(df_name, row):
    _id = str(df_name) + ','
    for idx, val in row.items():
        _id = _id + str(hash(_hash(val))) + ','
    return hash(_hash(_id))

def hash_row(df_name, row):
    _id = str(df_name) + ','
    for idx, val in row.items():
        if idx != 'sd':
            _id = _id + str(hash(_hash(val))) + ','
    return hash(_hash(_id))

def _hash(elem):
    str_elem = str(elem)
    str_elem_encoded = str_elem.encode('utf-8')
    return hash(int(hashlib.sha1(str_elem_encoded).hexdigest(), 16))

def get_sub_datasets(df, _range):
    df['sd'] = df['blockNumber'] / _range  # It categorizes the whole dataset in sub datasets
    df.loc[:, 'sd'] = df['sd'].apply(np.floor)
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
