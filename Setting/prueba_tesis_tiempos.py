import hashlib
import os, sys
import mariadb as maria
import swifter
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from EvaluatorTesis import EvaluatorTesis

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from datetime import datetime
from Ontological.NetDERKB import NetDERKB
from Ontological.NetDERChase import NetDERChase
from Ontological.NetDERQuery import NetDERQuery
from Ontological.NetDERTGD import NetDERTGD
from Ontological.NetDEREGD import NetDEREGD
from Ontological.Atom import Atom
from Ontological.GRE import GRE
from Ontological.GR import GR
from Ontological.Distinct import Distinct
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.RDBHomomorphism import RDBHomomorphism
from Ontological.ExpressionPlus import ExpressionPlus
from Ontological.Equal import Equal



config_db_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "configdb_tesis.json"
schema_path = os.path.dirname(os.path.realpath(__file__)) + '/' + "schema_tesis.xml"
entire_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "2000000to2999999_NormalTransaction.csv"
part_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "0to499_NormalTransaction"
dummy_tx_dataset = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_transactions"
ground_truth = os.path.dirname(os.path.realpath(__file__)) + '/' + "ground_truth.csv"
dummy_my_grado_in_out = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_grado_in_out"
dummy_my_gasPrice_in_out = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_gasPrice_in_out"
dummy_my_balance_in_out = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_balance_in_out"
dummy_my_contracts_created_and_invoc = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_contracts_created_and_invoc"
dummy_my_contracts_created_and_transfer = os.path.dirname(os.path.realpath(__file__)) + '/' + "my_contracts_created_and_transfer"

tmax = 2

# ---------------------------------------------------------------------------

global inicio_kb
global fin_kb
global inicio_q
global fin_q
global inicio_di
global fin_din

def main():
    T1_CC = 200
    # 1 min -> 4bloques
    sds = [4, 120, 240, 480, 720, 960, 1440, 2880] # Every 1mins, 1 hr, 2hr, 3hr, 4hr, 6hr, 12hr
    # sub_datasets = 5760

    #   --------------------------
    #   GETTING & MERGING DATASETS
    #   --------------------------

    print('START DIGEST')
    df_transactionInfo = pd.read_csv(entire_tx_dataset)
    df_transactionInfo['value'] = df_transactionInfo['value'].astype(float)
    df_transactionInfo = df_transactionInfo.drop(columns=['timestamp', 'gasUsed', 'gasLimit'])
    df_transactionInfo = df_transactionInfo[df_transactionInfo["isError"] == 'None'].drop(columns=['isError']).drop_duplicates().reset_index(drop=True)
    df_transactionInfo = df_transactionInfo.sort_values(by='blockNumber').reset_index(drop=True)

    #   --------------------------
    #   Atoms, TGDs & EGDs
    #   --------------------------

    print('Atoms and TGDs - START')
    tgd_counter = 0
    atom_is_owner_a1_c1 = Atom('is_owner', [Variable('A1'), Variable('C1')])
    atom_different_accounts_a1_a2 = Distinct(Variable('A1'), Variable('A2'))
    atom_gre_block_numbers_b1_b = GRE(Variable('B1'), Variable('B'))
    atom_invoke_a2_c1 = Atom('invoke', [Variable('A2'), Variable('C1'), Variable('B1')])
    atom_invoke_a1_c1 = Atom('invoke', [Variable('A1'), Variable('C1'), Variable('B1')])
    atom_is_owner_a2_c1 = Atom('is_owner', [Variable('A2'), Variable('C1')])

    atom_grado_in = Atom('degree_in', [Variable('SD'), Variable('B'), Variable('A1'), Variable('G_in')])
    atom_grado_in_b1 = Atom('degree_in', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('G_in')])
    atom_threshold_grado_in = Atom('threshold_degree_in', [Variable('SD'), Variable('A1'), Variable('T_gr_in')])
    atom_gre_grado_in_threshold = GRE(Variable('G_in'), Variable('T_gr_in'))
    atom_gre_threshold_grado_in = GRE(Variable('T_gr_in'), Variable('G_in'))
    atom_warning_degree_in = Atom('warning_degree_in', [Variable('SD'), Variable('B'), Variable('A1')])

    # R2.1.A) grado_in(SD, B, A1, G_in) & threshold_grado_In(SD, A1, T_gr_in) & (G_in > T_gr_in) → warning_gin(SD, B, A1)
    tgd_grado_in_rule = NetDERTGD(rule_id=tgd_counter,
                                  ont_body=[atom_grado_in, atom_threshold_grado_in, atom_gre_grado_in_threshold],
                                  ont_head=[atom_warning_degree_in])
    tgd_counter += 1

    exp = ExpressionPlus(terms=[Variable('B'), Constant('1')])
    atom_next_block = Equal(Variable('B1'), exp)
    hyp_malicious_a1_b1_grado_in = Atom('hyp_malicious', [Variable('A1'), Variable('B1'), Constant('Grado_In_burst')])

    # R2.1.B) warning_gin(A1, B, SD) & grado_in(A1, SD, B1, G_in) & threshold_grado_In(A1,SD, T_gr_in) & (G_in < T_gr_in)  & (B1 = B+1) → hyp_malicioso(A1, B1)
    tgd_grado_in_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                            ont_body=[atom_warning_degree_in, atom_grado_in_b1, atom_threshold_grado_in,
                                                      atom_gre_threshold_grado_in, atom_next_block],
                                            ont_head=[hyp_malicious_a1_b1_grado_in])
    tgd_counter += 1

    atom_grado_out = Atom('degree_out', [Variable('SD'), Variable('B'), Variable('A1'), Variable('G_out')])
    atom_grado_out_b1 = Atom('degree_out', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('G_out')])
    atom_threshold_grado_out = Atom('threshold_degree_out', [Variable('SD'), Variable('A1'), Variable('T_gr_out')])
    atom_gre_grado_out_threshold = GRE(Variable('G_out'), Variable('T_gr_out'))
    atom_gre_threshold_grado_out = GRE(Variable('T_gr_out'), Variable('G_out'))
    atom_warning_degree_out = Atom('warning_degree_out', [Variable('SD'), Variable('B'), Variable('A1')])

    # R2.2.A) grado_out(SD, B, A1, G_out) & threshold_grado_out(SD, A1, T_gr_out) & (G_out > T_gr_out) → warning_gout(SD, B, A1)
    tgd_grado_out_rule = NetDERTGD(rule_id=tgd_counter,
                                   ont_body=[atom_grado_out, atom_threshold_grado_out, atom_gre_grado_out_threshold],
                                   ont_head=[atom_warning_degree_out])
    tgd_counter += 1

    # R2.2.B) warning_gout(SD, B, A1) & grado_out(SD, B1, A1, G_out) & threshold_grado_out(SD, A1, T_gr_out) & (G_out < T_gr_out)  & (B1 = B+1) → hyp_malicioso(A1, B1)
    hyp_malicious_a1_b1_grado_out = Atom('hyp_malicious', [Variable('A1'), Variable('B1'), Constant('Grado_Out_burst')])
    tgd_grado_out_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                             ont_body=[atom_warning_degree_out, atom_grado_out_b1,
                                                       atom_threshold_grado_out,
                                                       atom_gre_threshold_grado_out, atom_next_block],
                                             ont_head=[hyp_malicious_a1_b1_grado_out])
    tgd_counter += 1

    atom_balance_in = Atom('balance_in', [Variable('SD'), Variable('B'), Variable('A1'), Variable('BAL_in')])
    atom_balance_in_b1 = Atom('balance_in', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('BAL_in')])
    atom_threshold_balance_in = Atom('threshold_balance_in', [Variable('SD'), Variable('A1'), Variable('T_BAL_in')])
    atom_gre_balance_in_threshold = GRE(Variable('BAL_in'), Variable('T_BAL_in'))
    atom_gre_threshold_balance_in = GRE(Variable('T_BAL_in'), Variable('BAL_in'))
    atom_warning_balance_in = Atom('warning_balance_in', [Variable('SD'), Variable('B'), Variable('A1')])

    # R.3.1.A) balance_in(SD, B, A1, Bal_in) & threshold_balance_in(SD, A1, T_bal_in) & (Bal_in > T_bal_in) → warning_bin(SD,B,A)
    tgd_balance_in_rule = NetDERTGD(rule_id=tgd_counter,
                                    ont_body=[atom_balance_in, atom_threshold_balance_in,
                                              atom_gre_balance_in_threshold],
                                    ont_head=[atom_warning_balance_in])
    tgd_counter += 1

    # R.3.1.B) warning_balance_in(SD, B, A) & balance_in(SD, B1, A, Bal_in) & threshold_balance_in(SD, A, T_bal_in) & (Bal_in < T_bal_in) & (B1 = B+1) → hyp_malicioso(A, B1)
    hyp_malicious_a1_b1_balance_in = Atom('hyp_malicious',
                                          [Variable('A1'), Variable('B1'), Constant('Balance_In_burst')])
    tgd_balance_in_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                              ont_body=[atom_warning_balance_in, atom_balance_in_b1,
                                                        atom_threshold_balance_in,
                                                        atom_gre_threshold_balance_in, atom_next_block],
                                              ont_head=[hyp_malicious_a1_b1_balance_in])
    tgd_counter += 1

    atom_balance_out = Atom('balance_out', [Variable('SD'), Variable('B'), Variable('A1'), Variable('BAL_out')])
    atom_balance_out_b1 = Atom('balance_out', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('BAL_out')])
    atom_threshold_balance_out = Atom('threshold_balance_out', [Variable('SD'), Variable('A1'), Variable('T_BAL_out')])
    atom_gre_balance_out_threshold = GRE(Variable('BAL_out'), Variable('T_BAL_out'))
    atom_gre_threshold_balance_out = GRE(Variable('T_BAL_out'), Variable('BAL_out'))
    atom_warning_balance_out = Atom('warning_balance_out', [Variable('SD'), Variable('B'), Variable('A1')])

    # R.3.2.A) balance_out(SD, B, A, Bal_out) & threshold_balance_out(SD, A, T_bal_out) & (Bal_out > T_bal_out) → warning_bout(SD, B, A)
    tgd_balance_out_rule = NetDERTGD(rule_id=tgd_counter,
                                     ont_body=[atom_balance_out, atom_threshold_balance_out,
                                               atom_gre_balance_out_threshold],
                                     ont_head=[atom_warning_balance_out])

    tgd_counter += 1

    hyp_malicious_a1_b1_balance_out = Atom('hyp_malicious',
                                           [Variable('A1'), Variable('B1'), Constant('Balance_Out_burst')])
    # R.3.2.B) warning_bout(SD, B, A) & balance_out(SD, B1, A, Bal_out) & threshold_balance_out(SD, A, T_bal_out) & (Bal_out < T_bal_out) & (B1 = B+1) → hyp_malicioso(A, B1)
    tgd_balance_out_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                               ont_body=[atom_warning_balance_out, atom_balance_out_b1,
                                                         atom_threshold_balance_out,
                                                         atom_gre_threshold_balance_out, atom_next_block],
                                               ont_head=[hyp_malicious_a1_b1_balance_out])
    tgd_counter += 1

    atom_gasPrice_in = Atom('gasPrice_in', [Variable('SD'), Variable('B'), Variable('A1'), Variable('GP_in')])
    atom_gasPrice_in_b1 = Atom('gasPrice_in', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('GP_in')])
    atom_threshold_gasPrice_in = Atom('threshold_gasPrice_in', [Variable('SD'), Variable('A1'), Variable('T_GP_in')])
    atom_gre_gasPrice_in_threshold = GRE(Variable('GP_in'), Variable('T_GP_in'))
    atom_gre_threshold_gasPrice_in = GRE(Variable('T_GP_in'), Variable('GP_in'))
    atom_warning_gasPrice_in = Atom('warning_gasPrice_in', [Variable('SD'), Variable('B'), Variable('A1')])

    # R.4.1.A) gasPrice_in(SD, B, A1,  GP_in) & threshold_gp_in(SD, A1, TGP_in) & (GP_in > Tht_GP_in) → warning_gp_in(SD, B, A1)
    tgd_gasPrice_in_rule = NetDERTGD(rule_id=tgd_counter,
                                     ont_body=[atom_gasPrice_in, atom_threshold_gasPrice_in,
                                               atom_gre_gasPrice_in_threshold],
                                     ont_head=[atom_warning_gasPrice_in])
    tgd_counter += 1

    hyp_malicious_a1_b1_gasPrice_in = Atom('hyp_malicious',
                                           [Variable('A1'), Variable('B1'), Constant('GasPrice_In_burst')])

    # R.4.1.B)  warning_gp_in(SD, B, A1) & gasPrice_in(SD, B1, A1,  GP_in) & threshold_gp_in(A1,SD, TGP_in) & (GP_in < Tht_GP_in) & (B1 = B+1) → hyp_malicioso(A1, B1)
    tgd_gasPrice_in_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                               ont_body=[atom_warning_gasPrice_in, atom_gasPrice_in_b1,
                                                         atom_threshold_gasPrice_in,
                                                         atom_gre_threshold_gasPrice_in, atom_next_block],
                                               ont_head=[hyp_malicious_a1_b1_gasPrice_in])
    tgd_counter += 1

    atom_gasPrice_out = Atom('gasPrice_out', [Variable('SD'), Variable('B'), Variable('A1'), Variable('GP_out')])
    atom_gasPrice_out_b1 = Atom('gasPrice_out', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('GP_out')])
    atom_threshold_gasPrice_out = Atom('threshold_gasPrice_out', [Variable('SD'), Variable('A1'), Variable('T_GP_out')])
    atom_gre_gasPrice_out_threshold = GRE(Variable('GP_out'), Variable('T_GP_out'))
    atom_gre_threshold_gasPrice_out = GRE(Variable('T_GP_out'), Variable('GP_out'))
    atom_warning_gasPrice_out = Atom('warning_gasPrice_out', [Variable('SD'), Variable('B'), Variable('A1')])

    # R.4.1.A) gasPrice_out(SD, B, A1,  GP_out) & threshold_gp_out(SD, A1, TGP_out) & (GP_out > Tht_GP_out) → warning_gp_out(SD, B, A1)
    tgd_gasPrice_out_rule = NetDERTGD(rule_id=tgd_counter,
                                      ont_body=[atom_gasPrice_out, atom_threshold_gasPrice_out,
                                                atom_gre_gasPrice_out_threshold],
                                      ont_head=[atom_warning_gasPrice_out])
    tgd_counter += 1

    # R.4.1.B)  warning_gp_out(SD, B, A1) & gasPrice_out(SD, B1, A1,  GP_out) & threshold_gp_out(A1,SD, TGP_out) & (GP_out < Tht_GP_out) & (B1 = B+1) → hyp_malicioso(A1, B1)
    hyp_malicious_a1_b1_gasPrice_out = Atom('hyp_malicious',
                                            [Variable('A1'), Variable('B1'), Constant('GasPrice_Out_burst')])
    tgd_gasPrice_out_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                                ont_body=[atom_warning_gasPrice_out, atom_gasPrice_out_b1,
                                                          atom_threshold_gasPrice_out,
                                                          atom_gre_threshold_gasPrice_out, atom_next_block],
                                                ont_head=[hyp_malicious_a1_b1_gasPrice_out])
    tgd_counter += 1

    # R6.1 hyp_malicioso(A1, B, M) & es_owner(A1, C1) & A1 != A2 & invoca(A2, C1, B1) & (B < B1) → hyp_misma_persona(A1, A2, B1, M)
    atom_hyp_malicious_a1_b_m = Atom('hyp_malicious', [Variable('A1'), Variable('B'), Variable('M')])
    atom_same_person_a1_a2_b1_m = Atom('hyp_same_person',
                                       [Variable('A1'), Variable('A2'), Variable('B1'), Variable('M')])
    tgd_invoke_account_rule = NetDERTGD(rule_id=tgd_counter,
                                        ont_body=[
                                            atom_hyp_malicious_a1_b_m,
                                            atom_is_owner_a1_c1,
                                            atom_invoke_a2_c1, atom_gre_block_numbers_b1_b,
                                            atom_different_accounts_a1_a2], ont_head=[
            atom_same_person_a1_a2_b1_m])
    tgd_counter += 1

    # R6.3 hyp_malicioso(A1, B, M) & invoca(A1, C1, B1) & (B < B1) & es_owner(A2, C1) & (A1 != A2)  →  hyp_malicioso(A2, B1, M)
    atom_hyp_malicious_a2_b1_m = Atom('hyp_malicious', [Variable('A2'), Variable('B1'), Variable('M')])
    tgd_invoke_malicious_rule = NetDERTGD(rule_id=tgd_counter,
                                          ont_body=[atom_hyp_malicious_a1_b_m,
                                                    atom_invoke_a1_c1, atom_is_owner_a2_c1,
                                                    atom_gre_block_numbers_b1_b,
                                                    atom_different_accounts_a1_a2],
                                          ont_head=[atom_hyp_malicious_a2_b1_m])
    tgd_counter += 1

    atom_hyp_sc_malicioso_c1 = Atom('hyp_malicious', [Variable('C1'), Variable('B'), Variable('M')])
    atom_is_owner_a1_c2 = Atom('is_owner', [Variable('A1'), Variable('C2')])
    atom_invoke_a2_c2 = Atom('invoke', [Variable('A2'), Variable('C2'), Variable('B1')])
    atom_different_contracts_c1_c2 = Distinct(Variable('C1'), Variable('C2'))

    # R6.2 hyp_malicioso(C1, B, M) & es_owner(A1, C1) & es_owner(A1, C2) & C1 != C2 & invoca(A2, C2, B1) & (A1 != A2) & (B < B1) → hyp_misma_persona(A1, A2, B1, M)
    tgd_invoke_contract_rule = NetDERTGD(rule_id=tgd_counter,
                                         ont_body=[atom_hyp_sc_malicioso_c1,
                                                   atom_is_owner_a1_c1, atom_is_owner_a1_c2,
                                                   atom_invoke_a2_c2, atom_different_contracts_c1_c2,
                                                   atom_gre_block_numbers_b1_b, atom_different_accounts_a1_a2],
                                         ont_head=[
                                             Atom('hyp_same_person',
                                                  [Variable('A1'), Variable('A2'), Variable('B1'), Variable('M')])])
    tgd_counter += 1

    # T1 = 10,000, T2 = T3 = 0.5,
    # T2’ = T2*size(SC_SET)
    # T3’ = T3*size(SC_SET)

    # R7.1.A) contracts_created(SD, B, A1, Contratos_creados) & (T1 < Contratos_creados) → Warning_cc(SD, B, A1)
    atom_contracts_created = Atom('contracts_created',
                                  [Variable('SD'), Variable('B'), Variable('A1'), Variable('Contratos_creados')])
    atom_gr_contracts_created = GR(Variable('Contratos_creados'), Constant(T1_CC))
    atom_warning_contracts_created = Atom('warning_contracts_created', [Variable('SD'), Variable('B'), Variable('A1')])
    atom_warning_contracts_created_sd1 = Atom('warning_contracts_created',
                                              [Variable('SD1'), Variable('B'), Variable('A1')])
    atom_invocaciones = Atom('invocaciones', [Variable('SD'), Variable('B1'), Variable('A1'), Variable('Invocaciones')])
    atom_threshold_invocaciones = Atom('threshold_invocaciones',
                                       [Variable('SD'), Variable('B1'), Variable('A1'), Variable('thr_invocaciones')])
    atom_gr_threshold_invocaciones = GR(Variable('thr_invocaciones'), Variable('Invocaciones'))

    tgd_contracts_created_rule = NetDERTGD(rule_id=tgd_counter,
                                           ont_body=[atom_contracts_created, atom_gr_contracts_created],
                                           ont_head=[atom_warning_contracts_created])
    tgd_counter += 1

    # R7.1.B) Warning_cc(SD1, B, A1) & invocaciones(SD, B1, A1, Invocaciones) & threshold_invocaciones(SD, B1, A1,T2’) & Invocaciones < T2’  --> hyp_malicioso(A1, B1)
    atom_hyp_malicious_a1_b1_useless_invoc = Atom('hyp_malicious',
                                                  [Variable('A1'), Variable('B1'), Constant('Useless_invoc')])
    tgd_invocaciones_rule = NetDERTGD(rule_id=tgd_counter,
                                      ont_body=[atom_warning_contracts_created_sd1, atom_invocaciones,
                                                atom_threshold_invocaciones, atom_gr_threshold_invocaciones],
                                      ont_head=[atom_hyp_malicious_a1_b1_useless_invoc])

    atom_transferencias = Atom('transferencias',
                               [Variable('SD'), Variable('B1'), Variable('A1'), Variable('Valor_transferido')])
    atom_threshold_transferencias = Atom('threshold_transferencias',
                                         [Variable('SD'), Variable('B1'), Variable('A1'),
                                          Variable('thr_transferencias')])
    atom_gr_threshold_transferencias = GR(Variable('thr_transferencias'), Variable('Valor_transferido'))

    # R7.1.C) Warning_cc(SD1, B, A1) & transferencias(SD, B1, A1, Valor_transferido) & threshold_transferencias(SD, B1, A1, T3’) & (Valor_transferido < T3’) --> hyp_malicioso(A1,B1)

    atom_hyp_malicious_a1_b1_useless_transf = Atom('hyp_malicious',
                                                   [Variable('A1'), Variable('B1'), Constant('Useless_transf')])
    tgd_transferencias_rule = NetDERTGD(rule_id=tgd_counter,
                                        ont_body=[atom_warning_contracts_created_sd1, atom_transferencias,
                                                  atom_threshold_transferencias, atom_gr_threshold_transferencias],
                                        ont_head=[atom_hyp_malicious_a1_b1_useless_transf])

    tgd_counter += 1

    # hyp_misma_persona(A1,A2, B2) & hyp_malicioso(C1, B) & es_owner(A1,C1) & es_owner(A1,C2) & invoca(A3,C2, B1) & (C1 != C2) & (B2 < B1) & (B < B1) → A2 = A3
    # egd1 = NetDEREGD(rule_id=tgd_counter,
    #                  ont_body=[atom_hyp_same_person_b2, atom_hyp_sc_malicioso_c1, atom_is_owner_a1_c1, atom_is_owner_a1_c2,
    #                            atom_invoke_a3_c2_b1, atom_different_contracts_c1_c2, atom_gre_block_numbers_b1_b2, atom_gre_block_numbers_b1_b],
    #                  head=[Variable('A2'), Variable('A3')])
    #
    # tgd_counter += 1

    # hyp_misma_persona(A1,A2,B2) & hyp_malicioso(A1, B) & invoca(A3,C1, B1) & es_owner(A1,C1) & (B2 < B1) & (B < B1) → A2 = A3
    # egd2 = NetDEREGD(rule_id=tgd_counter, ont_body=[atom_hyp_same_person_b2, atom_hyp_malicioso_a1_b, atom_invoke_a3_c1_b1, atom_is_owner_a1_c1,
    #                                       atom_different_contracts_c1_c2,
    #                                       atom_gre_block_numbers_b1_b, atom_gre_block_numbers_b1_b2],
    #                  head=[Variable('A2'), Variable('A3')])
    #

    # R6.4 hyp_malicioso(A1,B, M) → E A2 hyp_misma_persona_By_transitivity(A1,A2(null), B)
    atom_same_person_by_transitivity = Atom('hyp_same_person_by_transitivity',
                                            [Variable('A1'), Variable('A2'), Variable('B')])

    tgd_exist_same_account = NetDERTGD(rule_id=tgd_counter,
                                       ont_body=[atom_hyp_malicious_a1_b_m],
                                       ont_head=[atom_same_person_by_transitivity])
    tgd_counter += 1

    # # hyp_misma_persona_By_transitivity(A1,A2(null), B) & invoca(A3,C1, B1) & es_owner(A1,C1) & (B < B1) → A2(null) = A3
    atom_invoke_a3_c1_b1 = Atom('invoke', [Variable('A3'), Variable('C1'), Variable('B1')])

    egd3 = NetDEREGD(rule_id=tgd_counter, ont_body=[atom_same_person_by_transitivity,
                                                    atom_invoke_a3_c1_b1,
                                                    atom_is_owner_a1_c1, atom_gre_block_numbers_b1_b],
                     head=[Variable('A2'), Variable('A3')])

    tgd_counter += 1

    # hyp_malicious(A1,B, M) & hyp_same_person_by_transitivity(A1,A2,B1) -> hyp_malicious(A2, B1, 'By_transitivity')
    atom_same_person_a1_a2_b1_m = atom_same_person_a1_a2_b1_m
    tgd_same_person_malicious_v2_rule = NetDERTGD(rule_id=tgd_counter,
                                                  ont_body=[atom_hyp_malicious_a1_b_m,
                                                            atom_same_person_by_transitivity],
                                                  ont_head=[(Atom('hyp_malicious', [Variable('A2'), Variable('B1'),
                                                                                    Constant('By_transitivity')]))])

    tgd_counter += 1

    # hyp_malicious(A1,B, M) & hyp_same_person(A1,A2,B1) -> hyp_malicious(A2, B1)
    atom_same_person_a1_a2_b1_m = atom_same_person_a1_a2_b1_m
    tgd_same_person_malicious_v1_rule = NetDERTGD(rule_id=tgd_counter,
                                                  ont_body=[atom_hyp_malicious_a1_b_m, atom_same_person_a1_a2_b1_m],
                                                  ont_head=[(Atom('hyp_malicious', [Variable('A2'), Variable('B1'),
                                                                                    Constant('Same_person')]))])

    tgd_counter += 1

    burst_tgds = [
        tgd_grado_in_rule,
        tgd_grado_in_malicious_rule,
        tgd_grado_out_rule,
        tgd_grado_out_malicious_rule,
        tgd_balance_in_rule,
        tgd_balance_in_malicious_rule,
        tgd_balance_out_rule,
        tgd_balance_out_malicious_rule,
        tgd_gasPrice_in_rule,
        tgd_gasPrice_in_malicious_rule,
        tgd_gasPrice_out_rule,
        tgd_gasPrice_out_malicious_rule]

    same_accounts_tgds = [
        tgd_invoke_account_rule,
        tgd_invoke_malicious_rule,
        tgd_invoke_contract_rule,
        tgd_exist_same_account,
        tgd_same_person_malicious_v1_rule,
        tgd_same_person_malicious_v2_rule]

    useless_contracts_creation = [
        tgd_contracts_created_rule,
        tgd_invocaciones_rule,
        tgd_transferencias_rule]

    egd_same_accounts = [egd3]
    burst_and_same_accounts = burst_tgds + same_accounts_tgds
    same_accounts_and_useless_creation = same_accounts_tgds + useless_contracts_creation
    useless_creation_and_burst = useless_contracts_creation + burst_tgds
    all_tgds = burst_tgds + same_accounts_tgds + useless_creation_and_burst

    print('Atoms and TGDs - END')

    #   --------------------------
    #   DATA DIGESTION
    #   --------------------------

    with open('./results_time/times.csv', 'a') as result:
        result.write('subdataset,repiticion,tiempo_chase\n')

    for sub_datasets in sds:
        for repeticion in range(0, 10):
            kb = NetDERKB(data=set(), net_diff_graph=[], config_db=config_db_path, schema_path=schema_path, netder_tgds=all_tgds, netder_egds=egd_same_accounts, netdiff_lrules=[], netdiff_grules=[])

            print('\nsubdatasets: ' + str(sub_datasets))

            # Split in sub datasets
            df_transactionInfo_splitted = get_sub_datasets(df_transactionInfo, sub_datasets)

            #Get only the first 5 SDs
            df_transactionInfo_splitted = df_transactionInfo_splitted.loc[df_transactionInfo_splitted['sd'].isin(df_transactionInfo_splitted['sd'].unique()[0:5])]

            # # CONTRACT CREATION
            df_contracts_created = df_transactionInfo.filter(['blockNumber', 'from', 'creates'])
            df_contracts_created = df_contracts_created[(df_contracts_created['creates'] != 'none')]
            df_contracts_created = df_contracts_created.groupby(['blockNumber', 'from']).count().groupby(
                level=['from']).cumsum().reset_index()
            df_contracts_created['threshold'] = df_contracts_created['creates'] * 0.5
            df_contracts_created = df_contracts_created.rename(columns={'from': 'owner'})
            df_contracts_created = get_sub_datasets(df_contracts_created, sub_datasets)

            # INVOCACIONES
            print('Digest - INVOCACIONES')
            df_creaciones = df_transactionInfo[(df_transactionInfo['creates'] != 'none')].filter(
                ['from', 'creates', 'blockNumber']).reset_index(
                drop=True)  # Me quedo con los contratos creados y su creador
            df_invocaciones = df_transactionInfo[(df_transactionInfo['callingFunction'] != '0x')].filter(
                ['blockNumber', 'to'])  # Me quedo con las invocaciones y a quién se dirige.
            df_sin_invocaciones = df_creaciones.loc[~df_creaciones['creates'].isin(df_invocaciones['to'])].filter(
                ['from', 'blockNumber'])  # Contratos creados pero no invocados
            df_creaciones = df_creaciones.drop(columns=['blockNumber'])
            df_invocaciones = df_invocaciones.loc[df_invocaciones['to'].isin(df_creaciones['creates'])].reset_index(
                drop=True)  # Filtro las invocaciones dentro de los contratos creados
            df_creaciones = df_creaciones.set_index(df_creaciones['creates'], drop=True).drop(
                columns=['creates']).rename(
                columns={'from': 'owner'})  # Setteo el address del contrato como indice
            df_invocaciones_aux = df_invocaciones.join(df_creaciones, on='to').drop(columns=[
                'to'])  # Hago join entre el address del contrato y el contrato invocado, me quedarían todas las veces que se invoco a cada contrato, y el owner
            df_invocaciones_aux['#invocations'] = 0
            df_invocaciones = df_invocaciones_aux.groupby(['blockNumber', 'owner']).count().groupby(level=['owner'],
                                                                                                    sort=False).cumsum().reset_index()  # Esto me va a generr repetidos, que nos sirve ya que es la cantidad de veces que se llamo al contrto
            # Filtro los dueños cuyos contratos no fueron invocados
            df_sin_invocaciones = df_sin_invocaciones.loc[
                ~df_sin_invocaciones['from'].isin(df_invocaciones['owner'])].drop_duplicates(subset='from',
                                                                                             keep='first').reset_index(
                drop=True)
            df_sin_invocaciones['#invocations'] = 0
            df_sin_invocaciones = df_sin_invocaciones.rename(columns={'from': 'owner'})
            df_invocaciones = pd.concat([df_invocaciones, df_sin_invocaciones]).reset_index(drop=True)
            df_invocaciones_atom = get_sub_datasets(df_invocaciones, sub_datasets)

            # TRANSFERENCIAS
            print('Digest - TRANSFERENCIAS')
            # Me quedo con los creates, ya que nos interesan los dueños de los contratos a los que tenemos acceso y no los contratos.
            df_creaciones = df_transactionInfo[(df_transactionInfo['creates'] != 'none')].filter(
                ['from', 'creates', 'blockNumber']).reset_index(drop=True)
            df_transferencias = df_transactionInfo.filter(['blockNumber', 'to', 'value']).reset_index(
                drop=True)  # Me quedo con las transferencias de valor
            df_transferencias = df_transferencias.loc[
                df_transferencias['to'].isin(df_creaciones['creates'])].reset_index(
                drop=True)  # Me quedo con las transf. a contratos creados.

            df_sin_transferencias = df_creaciones.loc[~df_creaciones['creates'].isin(df_transferencias['to'])].filter(
                ['from', 'blockNumber'])  # Contratos creados pero no transferidos
            df_creaciones = df_creaciones.drop(columns=['blockNumber'])

            df_creaciones = df_creaciones.set_index(df_creaciones['creates'], drop=True).drop(
                columns=['creates']).rename(
                columns={'from': 'owner'})  # Filtro las transf. dentro de los contratos creados
            df_transferencias_aux = df_transferencias.join(df_creaciones,
                                                           on='to')  # Hago join entre el address del contrato y el contrato transferido, me queda todas las transferencias a cada contrato y su owner.
            df_transferencias = df_transferencias_aux.groupby(['blockNumber', 'owner']).sum().groupby(level=['owner'],
                                                                                                      sort=False).cumsum().reset_index()  # Cuento cuanto queda de valor

            df_sin_transferencias = df_sin_transferencias.loc[
                ~df_sin_transferencias['from'].isin(df_transferencias['owner'])].drop_duplicates(subset='from',
                                                                                                 keep='first').reset_index(
                drop=True)
            df_sin_transferencias['value'] = 0
            df_sin_transferencias = df_sin_transferencias.rename(columns={'from': 'owner'})

            df_transferencias = pd.concat([df_transferencias, df_sin_transferencias]).reset_index(drop=True)
            df_transferencias_atom = get_sub_datasets(df_transferencias, sub_datasets)

            # Calculo de threshold para invocaciones
            print('Digest - THR INVOCACIONES')
            # El siguiente codigo realiza lo siguiente. El threshold de invocaciones/transferencias depende de la cantidad de contratos creados por una cuenta
            # Para esto primero se calcula los contratos creados por una cuenta. Lo que se hace es, cada vez que la cuenta X crea un contrato en el bloque BN,
            # se crea un atomo que la cuenta X en el bloque BN creo 1 contrato. Luego si en el BN5 X crea un nuevo contrato, se cuenta que en el BN5
            # X creo 2 contratos. Ahora, el threshold T1 de las invocaciones es en función a los contratos creados. Esta función es T1 = contratos_creados*0.5.
            # Pero dado que el algoritmo original tomaba una foto estática de la blockchain y en este caso es dinámica (ingresan de a subdatasets), por cada subdataset nuevo,
            # en particular por cada bloque nuevo, se re computa el threshold. Entonces si en el BN1 hay una invocacion a un contrato de X, se
            # busca el último BN donde se computo lso contratos creados por X, se lo multiplica por 0.5 devolviendo T1 y se chequea que la cantidad de invocaciones
            # sea mayor a T1. Ahora si se invoca a un contrato creado por X en el bloque BN8, la cantidad de contratos creados por X fue computada en BN5. O sea 2.
            # Es importante remarcar que los thresholds en este caso se hacen por block number y no por sd.

            df_threshold_invocaciones = pd.merge(df_contracts_created, df_invocaciones_atom, how="inner", on=["owner"])
            df_threshold_invocaciones = df_threshold_invocaciones[
                df_threshold_invocaciones['blockNumber_x'] <= df_threshold_invocaciones['blockNumber_y']]
            df_threshold_invocaciones = df_threshold_invocaciones.groupby(
                ['sd_y', 'blockNumber_y', 'owner']).max().reset_index().filter(
                ['sd_y', 'blockNumber_y', 'owner', 'blockNumber_x', 'threshold'])
            # df_threshold_invocaciones['threshold'] = df_threshold_invocaciones['threshold'] * df_threshold_invocaciones[
            #     '#invocations']
            df_threshold_invocaciones = df_threshold_invocaciones.filter(
                ['sd_y', 'blockNumber_y', 'owner', 'threshold']).rename(
                columns={'sd_y': '2_sd', 'blockNumber_y': '3_blockNumber', 'owner': '4_address',
                         'threshold': '5_threshold_invocaciones'})

            # inicio_q = datetime.now()
            # df_threshold_invocaciones.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_invocaciones', row), axis=1).drop_duplicates().reset_index(drop=True)
            # fin_q = datetime.now()
            # print('tiempo 1:', (fin_q - inicio_q))

            df_threshold_invocaciones['1_primary_key'] = get_primary_keys(df_threshold_invocaciones,
                                                                          'threshold_invocaciones')
            df_threshold_invocaciones.drop_duplicates().reset_index(drop=True)

            # Calculo de threshold para transferencias
            print('Digest - THR TRANSFERENCIAS')
            df_threshold_transferencias = pd.merge(df_contracts_created, df_transferencias_atom, how="inner",
                                                   on=["owner"])
            df_threshold_transferencias = df_threshold_transferencias[
                df_threshold_transferencias['blockNumber_x'] <= df_threshold_transferencias['blockNumber_y']]
            df_threshold_transferencias = df_threshold_transferencias.groupby(
                ['sd_y', 'blockNumber_y', 'owner']).max().reset_index().filter(
                ['sd_y', 'blockNumber_y', 'owner', 'blockNumber_x', 'threshold'])

            df_threshold_transferencias = df_threshold_transferencias.filter(
                ['sd_y', 'blockNumber_y', 'owner', 'threshold']).rename(
                # Value deberia agaregarse para comparar el threshold
                columns={'sd_y': '2_sd', 'blockNumber_y': '3_blockNumber', 'owner': '4_address',
                         'threshold': '5_threshold_transferencias'})
            # df_threshold_transferencias['1_primary_key'] = df_threshold_transferencias.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_transferencias', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_threshold_transferencias['1_primary_key'] = get_primary_keys(df_threshold_transferencias,
                                                                            'threshold_transferencias')

            df_invocaciones_atom.rename(columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'owner': '4_address',
                                                 '#invocations': '5_invocaciones'},
                                        inplace=True)
            # df_invocaciones_atom['1_primary_key'] = df_invocaciones_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('invocaciones', row), axis=1).drop_duplicates().reset_index(drop=True)

            df_invocaciones_atom['1_primary_key'] = get_primary_keys(df_invocaciones_atom,
                                                                     'invocaciones')

            df_transferencias_atom.rename(columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'owner': '4_address',
                                                   'value': '5_value_transferido'},
                                          inplace=True)
            # df_transferencias_atom['1_primary_key'] = df_transferencias_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('transferencias', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_transferencias_atom['1_primary_key'] = get_primary_keys(df_transferencias_atom, 'transferencias')

            df_contracts_created_atoms = df_contracts_created.filter(['sd', 'blockNumber', 'owner', 'creates'])
            df_contracts_created_atoms = df_contracts_created_atoms.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'owner': '4_address',
                         'creates': '5_contracts_created'})
            # df_contracts_created_atoms['1_primary_key'] = df_contracts_created_atoms.swifter.apply(
            #     lambda row: hash_row_with_sd('contracts_created', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_contracts_created_atoms['1_primary_key'] = get_primary_keys(df_contracts_created_atoms,
                                                                           'contracts_created')

            # Get threshold and atoms per dataset per account

            degree = df_transactionInfo_splitted.filter(['sd', 'blockNumber', 'from', 'to'])

            df_degree_in = degree.groupby(['sd', 'blockNumber', 'to']).count().reset_index()
            # DEGREE_IN_THRESHOOLD
            print('Digest - DEGREE IN THRESHOLD')
            df_max_degree_in = df_degree_in.filter(['sd', 'from', 'to'])
            df_thr_degree_in = (df_max_degree_in.groupby(['sd', 'to']).max() * 0.8).reset_index()
            df_thr_degree_in.rename(columns={'sd': '2_sd', 'to': '3_address', 'from': '4_thr_degree_in'},
                                    inplace=True)
            # df_thr_degree_in['1_primary_key'] = df_thr_degree_in.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_degree_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_degree_in['1_primary_key'] = get_primary_keys(df_thr_degree_in, 'threshold_degree_in')

            df_degree_in.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'to': '4_address', 'from': '5_degree_in'},
                inplace=True)

            # DEGREE_IN
            print('Digest - DEGREE IN')
            # df_degree_in['1_primary_key'] = df_degree_in.swifter.apply(
            #     lambda row: hash_row_with_sd('degree_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_degree_in['1_primary_key'] = get_primary_keys(df_degree_in, 'degree_in')

            # DEGREE_OUT
            print('Digest - DEGREE OUT')
            df_degree_out = degree.groupby(['sd', 'blockNumber', 'from']).count().reset_index()

            # DEGREE_OUT_THRESHOOLD
            print('Digest - DEGREE THR OUT')
            max_degree_out = df_degree_out.filter(['sd', 'from', 'to'])
            df_thr_degree_out = (max_degree_out.groupby(['sd', 'from']).max() * 0.8).reset_index()
            df_thr_degree_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'to': '4_thr_degree_out'},
                                     inplace=True)
            # df_thr_degree_out['1_primary_key'] = df_thr_degree_out.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_degree_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_degree_out['1_primary_key'] = get_primary_keys(df_thr_degree_out, 'threshold_degree_out')

            df_degree_out.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'from': '4_address', 'to': '5_degree_out'},
                inplace=True)
            # df_degree_out['1_primary_key'] = df_degree_out.swifter.apply(
            #     lambda row: hash_row_with_sd('degree_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_degree_out['1_primary_key'] = get_primary_keys(df_degree_out, 'df_degree_out')

            # GAS_PRICE_OUT
            print('Digest - GAS PRICE OUT')
            df_gas_price_out_atom = df_transactionInfo_splitted.filter(
                ['sd', 'blockNumber', 'from', 'gasPrice']).drop_duplicates().reset_index(drop=True)
            df_gas_price_out_atom.rename(columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'from': '4_address',
                                                  'gasPrice': '5_gasPrice_out'},
                                         inplace=True)
            # df_gas_price_out_atom['1_primary_key'] = df_gas_price_out_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('gasPrice_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_gas_price_out_atom['1_primary_key'] = get_primary_keys(df_gas_price_out_atom, 'gasPrice_out')

            # GAS_PRICE_OUT_THRESHOLD
            print('Digest - GAS PRICE OUT THR')
            df_gas_price_out = df_transactionInfo_splitted.filter(
                ['sd', 'from', 'gasPrice']).drop_duplicates().reset_index(drop=True)
            df_thr_gasPrice_out = df_gas_price_out.groupby(['sd', 'from']).max().reset_index()
            df_thr_gasPrice_out['gasPrice'] = df_thr_gasPrice_out['gasPrice'] * 0.8
            df_thr_gasPrice_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'gasPrice': '4_thr_gasPrice_out'},
                                       inplace=True)
            # df_thr_gasPrice_out['1_primary_key'] = df_thr_gasPrice_out.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_gasPrice_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_gasPrice_out['1_primary_key'] = get_primary_keys(df_thr_gasPrice_out, 'threshold_gasPrice_out')

            # GAS_PRICE_IN
            print('Digest - GAS PRICE IN')
            df_gas_price_in_atom = df_transactionInfo_splitted.filter(
                ['sd', 'blockNumber', 'to', 'gasPrice']).drop_duplicates().reset_index(drop=True)
            df_gas_price_in_atom.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'to': '4_address', 'gasPrice': '5_gasPrice_in'},
                inplace=True)
            # df_gas_price_in_atom['1_primary_key'] = df_gas_price_in_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('gasPrice_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_gas_price_in_atom['1_primary_key'] = get_primary_keys(df_gas_price_in_atom, 'gasPrice_in')

            # GAS_PRICE_IN_THRESHOLD
            print('Digest - GAS PRICE IN THRESHOLD')
            df_gas_price_in = df_transactionInfo_splitted.filter(
                ['sd', 'to', 'gasPrice']).drop_duplicates().reset_index(drop=True)
            df_thr_gasPrice_in = df_gas_price_in.groupby(['sd', 'to']).max().reset_index()
            df_thr_gasPrice_in['gasPrice'] = df_thr_gasPrice_in['gasPrice'] * 0.8
            df_thr_gasPrice_in.rename(columns={'sd': '2_sd', 'to': '3_address', 'gasPrice': '4_thr_gasPrice_in'},
                                      inplace=True)
            # df_thr_gasPrice_in['1_primary_key'] = df_thr_gasPrice_in.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_gasPrice_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_gasPrice_in['1_primary_key'] = get_primary_keys(df_thr_gasPrice_in, 'threshold_gasPrice_in')

            #   BALANCE_OUT
            print('Digest - BALANCE OUT')
            df_balance_out_atom = df_transactionInfo_splitted.filter(
                ['sd', 'blockNumber', 'from', 'value']).drop_duplicates().reset_index(drop=True)
            df_balance_out_atom.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'from': '4_address', 'value': '5_balance_out'},
                inplace=True)
            # df_balance_out_atom['1_primary_key'] = df_balance_out_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('balance_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_balance_out_atom['1_primary_key'] = get_primary_keys(df_balance_out_atom, 'balance_out')

            #   BALANCE_OUT_THRESHOLD
            print('Digest - BALANCE OUT THR')
            df_balance_out = df_transactionInfo_splitted.filter(['sd', 'from', 'value']).drop_duplicates().reset_index(
                drop=True)
            df_thr_balance_out = df_balance_out.groupby(['sd', 'from']).max().reset_index()
            df_thr_balance_out['value'] = df_thr_balance_out['value'] * 0.8
            df_thr_balance_out['value'].apply(np.round)
            df_thr_balance_out.rename(columns={'sd': '2_sd', 'from': '3_address', 'value': '4_thr_balance_out'},
                                      inplace=True)
            # df_thr_balance_out['1_primary_key'] = df_thr_balance_out.swifter.apply(
            #     lambda row: hash_row_with_sd('threshold_balance_out', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_balance_out['1_primary_key'] = get_primary_keys(df_thr_balance_out, 'threshold_balance_out')

            #   BALANCE_IN
            print('Digest - BALANCE IN')
            df_balance_in_atom = df_transactionInfo_splitted.filter(
                ['sd', 'blockNumber', 'to', 'value']).drop_duplicates().reset_index(drop=True)
            df_balance_in_atom.rename(
                columns={'sd': '2_sd', 'blockNumber': '3_blockNumber', 'to': '4_address', 'value': '5_balance_in'},
                inplace=True)
            # df_balance_in_atom['1_primary_key'] = df_balance_in_atom.swifter.apply(
            #     lambda row: hash_row_with_sd('balance_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_balance_in_atom['1_primary_key'] = get_primary_keys(df_balance_in_atom, 'balance_in')

            #   BALANCE_IN_THRESHOLD
            print('Digest - BALANCE IN THR')
            df_balance_in = df_transactionInfo_splitted.filter(['sd', 'to', 'value']).drop_duplicates().reset_index(
                drop=True)
            df_thr_balance_in = df_balance_in.groupby(['sd', 'to']).max().reset_index()
            df_thr_balance_in['value'] = df_thr_balance_in['value'] * 0.8
            df_thr_balance_in.rename(columns={'sd': '2_sd', 'to': '3_address', 'value': '4_thr_balance_in'},
                                     inplace=True)
            # df_thr_balance_in['1_primary_key'] = df_thr_balance_in.swifter.apply(lambda row: hash_row_with_sd('threshold_balance_in', row), axis=1).drop_duplicates().reset_index(drop=True)
            df_thr_balance_in['1_primary_key'] = get_primary_keys(df_thr_balance_in, 'threshold_balance_in')

            # Gets contract creations
            df_contract_creation = df_transactionInfo_splitted[df_transactionInfo_splitted['creates'] != 'none']

            # Gets created contract addresses for is_contract
            df_contract_creation_addresses = df_contract_creation.filter(
                ['creates', 'sd']).drop_duplicates().reset_index(drop=True)
            df_contract_creation_addresses.rename(columns={'creates': '2_address'}, inplace=True)

            # Gets all accounts
            df_account_from = df_transactionInfo_splitted.filter(['from', 'sd'])
            df_account_from.rename(columns={'from': '2_address'}, inplace=True)
            df_account_to = df_transactionInfo_splitted.filter(['to', 'sd'])
            df_account_to.rename(columns={'to': '2_address'}, inplace=True)
            df_account = pd.concat([df_account_from, df_account_to, df_contract_creation_addresses],
                                   ignore_index=True).drop_duplicates().reset_index(drop=True)
            df_account.rename(columns={'from': '2_address'}, inplace=True)
            # df_account['1_primary_key'] = df_account.swifter.apply(lambda row: hash_row('account', row), axis=1)

            # Gets invocations
            df_invoke = df_transactionInfo_splitted[df_transactionInfo_splitted['callingFunction'] != '0x'].filter(
                ['from', 'to', 'blockNumber', 'sd']).drop_duplicates().reset_index(drop=True)
            df_invoke.columns = ['2_address', '3_address', '4_blockNumber', 'sd']
            df_invoke['1_primary_key'] = df_invoke.swifter.apply(lambda row: hash_row('invoke', row), axis=1)

            # Gets the owner of each contracts
            df_is_owner = df_contract_creation.filter(['from', 'creates', 'sd']).reset_index(drop=True)
            df_is_owner.columns = ['2_address', '3_address', 'sd']
            df_is_owner = df_is_owner.drop_duplicates().reset_index(drop=True)
            df_is_owner['1_primary_key'] = df_is_owner.swifter.apply(lambda row: hash_row('is_owner', row), axis=1)

            df_sds = pd.DataFrame({"sd": df_transactionInfo_splitted['sd'].unique()})

            print('Digest - END')

            query2 = NetDERQuery(ont_cond=[atom_hyp_malicious_a1_b_m], time=(tmax, tmax))
            actual_query = query2
            cur = kb.get_connection().cursor()
            kb.get_connection().commit()
            engine = create_engine("mariadb+mariadbconnector://user:@127.0.0.1:3306/test_tesis")

            print('CHASE - START')
            print("NetDER Query")
            print(actual_query)
            print('-----')

            df_threshold_invocaciones['3_blockNumber'] = df_threshold_invocaciones['3_blockNumber'].astype(str)
            df_threshold_transferencias['3_blockNumber'] = df_threshold_transferencias['3_blockNumber'].astype(str)
            df_transferencias_atom['3_blockNumber'] = df_transferencias_atom['3_blockNumber'].astype(str)
            df_invocaciones_atom['3_blockNumber'] = df_invocaciones_atom['3_blockNumber'].astype(str)
            df_contracts_created_atoms['3_blockNumber'] = df_contracts_created_atoms['3_blockNumber'].astype(str)
            df_invoke['4_blockNumber'] = df_invoke['4_blockNumber'].astype(str)
            df_degree_in['3_blockNumber'] = df_degree_in['3_blockNumber'].astype(str)
            df_degree_out['3_blockNumber'] = df_degree_out['3_blockNumber'].astype(str)
            df_gas_price_out_atom['3_blockNumber'] = df_gas_price_out_atom['3_blockNumber'].astype(str)
            df_gas_price_in_atom['3_blockNumber'] = df_gas_price_in_atom['3_blockNumber'].astype(str)
            df_balance_out_atom['3_blockNumber'] = df_balance_out_atom['3_blockNumber'].astype(str)
            df_balance_in_atom['3_blockNumber'] = df_balance_in_atom['3_blockNumber'].astype(str)

            for sd in df_sds['sd'].unique():
                print('\nsd: ' + str(sd))

                # Clean old tables not used in future TGDs
                kb.clean_tables(kb.get_connection().cursor())

                # Degree_in(SD, BlockNumber, Address, degree_in)
                df_degree_in_sd = df_degree_in[df_degree_in['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_degree_in'])

                # Degree_out(SD, BlockNumber, Address, degree_out)
                df_degree_out_sd = df_degree_out[df_degree_out['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_degree_out'])

                # Gas_price_out(SD, BlockNumber, Address, gas_price_out)
                df_gas_price_out_atom_sd = df_gas_price_out_atom[df_gas_price_out_atom['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_gasPrice_out'])

                # Gas_price_in(SD, BlockNumber, Address, gas_price_in)
                df_gas_price_in_atom_sd = df_gas_price_in_atom[df_gas_price_in_atom['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_gasPrice_in'])

                # Balance_out(SD, BlockNumber, Address, balance_out)
                df_balance_out_atom_sd = df_balance_out_atom[df_balance_out_atom['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_balance_out'])

                # Balance_in(SD, BlockNumber, Address, balance_in)
                df_balance_in_atom_sd = df_balance_in_atom[df_balance_in_atom['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_balance_in'])

                # threshold_invocaciones(SD, BlockNumber, Address, Threshold_invocaciones)
                df_threshold_invocaciones_sd = df_threshold_invocaciones[df_threshold_invocaciones['2_sd'] == sd].filter(
                    ['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_threshold_invocaciones'])

                # threshold_transferencias(SD, BlockNumber, Address, Threshold_transferencias)
                df_threshold_transferencias_sd = df_threshold_transferencias[df_threshold_transferencias['2_sd'] == sd].filter(
                    ['1_primary_key', '2_sd', '3_blockNumber', '4_address', '5_threshold_transferencias'])

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
                df_thr_gasPrice_out_sd = df_thr_gasPrice_out[df_thr_gasPrice_out['2_sd'] == sd].filter(
                    ['1_primary_key', '2_sd', '3_address', '4_thr_gasPrice_out'])

                #   threshold_gasPrice_in(SD, Address, GasPrice_in)
                df_thr_gasPrice_in_sd = df_thr_gasPrice_in[df_thr_gasPrice_in['2_sd'] == sd].filter(
                    ['1_primary_key', '2_sd', '3_address', '4_thr_gasPrice_in'])

                #   threshold_balance_in(SD, Address, Balance_In)
                df_thr_balance_in_sd = df_thr_balance_in[df_thr_balance_in['2_sd'] == sd].filter(['1_primary_key', '2_sd','3_address', '4_thr_balance_in'])

                #   threshold_balance_in(SD, Address, Balance_Out)
                df_thr_balance_out_sd = df_thr_balance_out[df_thr_balance_out['2_sd'] == sd].filter(['1_primary_key', '2_sd', '3_address', '4_thr_balance_out'])

                #   invoke(Address, Address, BlockNumber)
                df_invoke_sd = df_invoke[df_invoke['sd'] == sd].filter(['1_primary_key', '2_address', '3_address', '4_blockNumber'])

                #   Is_owner(Address, Address,)
                df_is_owner_sd = df_is_owner[df_is_owner['sd'] == sd].filter(['1_primary_key', '2_address', '3_address'])

                #   hyp_malicious
                # df_malicious_sd = df_malicious[df_malicious['sd'] == sd].filter(['1_primary_key', '2_address', '3_block_number'])

                df_threshold_invocaciones_sd.to_sql('threshold_invocaciones', con=engine, index=False, if_exists='append')
                df_threshold_transferencias_sd.to_sql('threshold_transferencias', con=engine, index=False, if_exists='append')
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
                df_degree_in_sd.to_sql('degree_in', con=engine, index=False, if_exists='append')
                df_degree_out_sd.to_sql('degree_out', con=engine, index=False, if_exists='append')
                df_gas_price_out_atom_sd.to_sql('gasPrice_out', con=engine, index=False, if_exists='append')
                df_gas_price_in_atom_sd.to_sql('gasPrice_in', con=engine, index=False, if_exists='append')
                df_balance_out_atom_sd.to_sql('balance_out', con=engine, index=False, if_exists='append')
                df_balance_in_atom_sd.to_sql('balance_in', con=engine, index=False, if_exists='append')

                chase = NetDERChase(kb, tmax)

                inicio_q = datetime.now()
                chase.answer_query(actual_query, 1)
                fin_q = datetime.now()

                if sd == df_sds['sd'].unique()[4]:
                    with open('./results_time/times.csv', 'a') as result:
                        result.write(str(sub_datasets) + ',' + str(repeticion) + ',' + str(fin_q - inicio_q)+'\n')

                # for ans in answers:
                #     for key in ans.keys():
                #         print("Variable", key, "instanciada con valor", ans[key].getValue())
                #
                # print(evaluator.evaluate(answers, df_account_sd, sd))
                #
                # iter = sd % 100
                # if (iter == 0):
                #     evaluator.save_results('total')
                #     evaluator.save_times(sd)

                print('NetDERChase.contador', NetDERChase.contador)
                print('tiempo de traduccion:', RDBHomomorphism.TRANSLATE_TIME)
                print('tiempo de construccion de homomorfismos', RDBHomomorphism.HOMOMORPH_BUILT_TIME)
                print('tiempo de ejecucion consulta SQL', RDBHomomorphism.HOMOMORPH_SQL_QUERY)
                print('tiempo para responder consulta:', (fin_q - inicio_q))

            cur.close()
            kb.close_connection()


def get_primary_keys(df, name):
    return [hash_row_2(name, x) for x in list(zip(*[df[col] for col in df]))]

def hash_row_with_sd(df_name, row):
    _id = str(df_name) + ','
    for idx, val in row.items():
        _id = _id + str(hash(_hash(val))) + ','
    return hash(_hash(_id))

def hash_row_2(df_name, row):
    _id = str(df_name) + ','
    for val in row:
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
    df['sd'] = df['sd'].astype(int)
    return df

def assert_column_name(df, name):
    assert name is not list(df.columns.values), str(name) + " is not in the columns)"

def delete_datasets():
    delete_dataset('./dataset/sds.csv')
    delete_dataset('./dataset/df_account.csv')
    delete_dataset('./dataset/df_threshold_invocaciones.csv')
    delete_dataset('./dataset/df_threshold_transferencias.csv')
    delete_dataset('./dataset/df_transferencias_atom.csv')
    delete_dataset('./dataset/df_invocaciones_atom.csv')
    delete_dataset('./dataset/df_contracts_created_atoms.csv')
    delete_dataset('./dataset/df_thr_degree_out.csv')
    delete_dataset('./dataset/df_thr_degree_in.csv')
    delete_dataset('./dataset/df_thr_gasPrice_out.csv')
    delete_dataset('./dataset/df_thr_gasPrice_in.csv')
    delete_dataset('./dataset/df_thr_balance_out.csv')
    delete_dataset('./dataset/df_thr_balance_in.csv')
    delete_dataset('./dataset/df_invoke.csv')
    delete_dataset('./dataset/df_is_owner.csv')
    delete_dataset('./dataset/df_degree_in.csv')
    delete_dataset('./dataset/df_degree_out.csv')
    delete_dataset('./dataset/df_gas_price_out_atom.csv')
    delete_dataset('./dataset/df_gas_price_in_atom.csv')
    delete_dataset('./dataset/df_balance_out_atom.csv')
    delete_dataset('./dataset/df_balance_in_atom.csv')

def delete_dataset(path):
    if os.path.exists(path):
        os.remove(path)

if __name__ == "__main__":
    main()
    exit(0)
