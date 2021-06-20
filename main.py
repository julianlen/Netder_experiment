import os
import sys
import pandas as pd
from Ontological.GRE import GRE

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import csv
import json
from Setting.DataIngestionModule import DataIngestionModule
from Ontological.NetDERKB import NetDERKB
from Ontological.NetDB import NetDB
from Ontological.NetDERChase import NetDERChase
from Ontological.NetDERQuery import NetDERQuery
from Ontological.NetDERTGD import NetDERTGD
from Ontological.Atom import Atom
from Ontological.Variable import Variable


def save_csv(csv_source, data):
    fieldnames = {}
    for key in data.keys():
        fieldnames[key] = key
    fieldnames = list(data.keys())
    with open(csv_source, 'a+', newline='') as csvfile:
        read_data = []
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if os.stat(csv_source).st_size > 0:
            reader = csv.DictReader(csvfile, fieldnames=fieldnames)
            read_data = list(reader)
        else:
            writer.writeheader()
        writer.writerows(read_data)
        writer.writerow(data)


# '''
# setting_values = {'fk_threshold': 0.3, 'tmax': 2, 'time_sim': 15, 'trending interval': portion.closed(0.5, 1),
# 					'cant_botnet': 1, 'sb_prob': 0.5, 'fn_prob': 0.1, 'malicious_prop': 0.25,
# 					'new_post_prob_mal': 0.1, 'new_post_prob_no_mal': 0.01, 'share_post_prob': 0.1,
# 					'csv_graph_location': '../graph_data/graph(n=1000, e=2000).csv',
# 					'results_loc': '../FakeNewsData/results/res_Liar.json'}'''
# '''
# setting_values = {'fk_threshold': 0.3, 'tmax': 2, 'time_sim': 15, 'trending_interval_lower': 0.5,
# 					'trending_interval_upper': 1, 'cant_botnet': 5, 'sb_prob': 1, 'fn_prob': 0.1, 'malicious_prop': 0.25,
# 					'new_post_prob_mal': 0.3, 'new_post_prob_no_mal': 0.01, 'share_post_prob': 0.99,
# 					'csv_graph_location': '../graph_data/graph(n=150, e=495).csv',
# 					'results_loc': '../FakeNewsData/results/res_Liar.json'}'''
#
# setting_values_collection = None
# with open('../setting_values.csv', newline='') as csvfile:
#     setting_values_collection = list(csv.DictReader(csvfile))
#
# inicio = datetime.now()
# setting_counter = 1
# programs = ['alpha', 'beta', 'alpha*']


#     total_botnets_fn_det = []

#     total_botnets_fn_det2 = []
#     for index in range(len(programs)):
#         total_botnets_fn_det.append(0)
#         total_botnets_fn_det2.append(0)
#     total_botnets_fn = 0
# result_eval = {"query_time": None, 'sett': setting_values['setting']}
#     reduced_result_eval = {}
#     reduced_result_eval2 = [{}, {}, {}]

# for rr in reduced_result_eval2:
#     rr['prog'] = None
#     rr['sett'] = None
#     rr['bn_fn'] = None
#     rr['bn_fn_det'] = None
#     rr['bn_fn_det2'] = None
#     for header in get_csv_headers('../reduced_result_exp_headers.csv'):
#         rr[str(header)] = {'total_value': 0, 'total_samples': int(setting_values["cant_run"])}
def assert_column_name(df, name):
    assert (not name is list(df.columns.values))


def main():
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

    with open('./config.json') as f:
        setting_values = json.load(f)
    for run in range(int(setting_values["run_times"])):
        t_max = int(setting_values["t_max"])

        df = pd.read_csv(setting_values["dataset_location"])
        df['value'] = df['value'].astype(float)
        assert_column_name(df, 'gasPrice')
        assert_column_name(df, 'value')

        # This section should be an API or an external file, where a domain expert writes the rules.
        print('Dataset atoms')
        #   Dataset atoms
        atom_gas_price = Atom('GasPrice', [Variable(SD), Variable(ADDRESS), Variable(BLOCK_NUMBER), Variable(GAS_PRICE)])
        atom_in_balance = Atom('In_Balance',
                               [Variable(SD), Variable(ADDRESS), Variable(BLOCK_NUMBER), Variable(IN_BALANCE)])
        atom_out_balance = Atom('Out_Balance',
                                [Variable(SD), Variable(ADDRESS), Variable(BLOCK_NUMBER), Variable(OUT_BALANCE)])
        atom_in_degree = Atom('InDegree', [Variable(SD), Variable(ADDRESS), Variable(BLOCK_NUMBER), Variable(IN_DEGREE)])
        atom_out_degree = Atom('OutDegree',
                               [Variable(SD), Variable(ADDRESS), Variable(BLOCK_NUMBER), Variable(OUT_DEGREE)])
        atom_hyp = Atom('hyp_mal_account', [Variable(ADDRESS)])

        #   Threshold atoms
        print('Threshold atoms')
        atom_thr_in_degree = Atom('Thr_in_degree', [Variable(ADDRESS), Variable(SD), Variable(TH_DG_IN)])
        atom_thr_out_degree = Atom('Thr_out_degree', [Variable(ADDRESS), Variable(SD), Variable(TH_DG_OUT)])
        atom_thr_in_bal = Atom('Thr_in_bal', [Variable(ADDRESS), Variable(SD), Variable(TH_BAL_IN)])
        atom_thr_out_bal = Atom('Thr_out_bal', [Variable(ADDRESS), Variable(SD), Variable(TH_BAL_OUT)])
        atom_thr_gas_pr = Atom('Thr_gas_price', [Variable(ADDRESS), Variable(SD), Variable(TH_GAS_PR)])

        #   Greater Than (>) atoms
        print('Greater Than (>) atoms')
        atom_gp_greater_that = GRE(Variable(GAS_PRICE), Variable(TH_GAS_PR))
        atom_in_degree_greater_that = GRE(Variable(IN_DEGREE), Variable(TH_DG_IN))
        atom_out_degree_greater_that = GRE(Variable(OUT_DEGREE), Variable(TH_DG_OUT))
        atom_bal_in_greater_that = GRE(Variable(IN_BALANCE), Variable(TH_BAL_IN))
        atom_bal_out_greater_that = GRE(Variable(OUT_BALANCE), Variable(TH_BAL_OUT))

        # Rules
        print('Rules')
        tgd_gp_rule = NetDERTGD(rule_id=1, ont_body=[atom_gas_price, atom_thr_gas_pr, atom_gp_greater_that],
                                ont_head=[atom_hyp])
        tgd_in_bal_rule = NetDERTGD(rule_id=2, ont_body=[atom_in_balance, atom_thr_in_bal, atom_bal_in_greater_that],
                                    ont_head=[atom_hyp])
        tgd_out_bal_rule = NetDERTGD(rule_id=3,
                                     ont_body=[atom_out_balance, atom_thr_out_bal, atom_bal_out_greater_that],
                                     ont_head=[atom_hyp])
        tgd_in_degree_rule = NetDERTGD(rule_id=4,
                                       ont_body=[atom_in_degree, atom_thr_in_degree, atom_in_degree_greater_that],
                                       ont_head=[atom_hyp])
        tgd_out_degree_rule = NetDERTGD(rule_id=5,
                                        ont_body=[atom_out_degree, atom_thr_out_degree, atom_out_degree_greater_that],
                                        ont_head=[atom_hyp])

        query = NetDERQuery(exist_var=[], ont_cond=[atom_hyp],
                            time=(t_max, t_max))

        kb = NetDERKB([], net_db=NetDB(),
                      netder_tgds=[tgd_gp_rule, tgd_in_bal_rule, tgd_out_bal_rule,
                                   tgd_in_degree_rule,
                                   tgd_out_degree_rule],
                      netder_egds=[],
                      netdiff_lrules=[], netdiff_grules=[]
                      )

        DataIngestionModule(setting_values["dataset_location"], setting_values["sub_dataset"], kb)
        print('Entrando')
        #   NetDerKB = (D, G, E, P)
        # kb = NetDERKB(ont_data=dim.get_atoms(), net_db=NetDB(),
        #               netder_tgds=[tgd_gp_rule, tgd_in_bal_rule, tgd_out_bal_rule,
        #                            tgd_in_degree_rule,
        #                            tgd_out_degree_rule],
        #               netder_egds=[],
        #               netdiff_lrules=[], netdiff_grules=[]
        #               )
        print('Saliendo')
        chase = NetDERChase(kb, t_max)
        answers = chase.answer_query(query, 1)  # 1: One-shot Chase
        print(len(answers))
        print(len(df['from'].unique()))
        # print(answers[0])

        # ont_db = []
        # net_db = []
        # inicio_bdb = datetime.now()
        # for time in range(time_sim):
        #     print('time: ', time)
        #     logic_database = posts_db.ingest_data()
        #     ont_db.append(logic_database['ont_db'])
        #     net_db.append(logic_database['net_db'])
        # fin_bdb = datetime.now()

        # total_posts = 0
        # for posts in posts_db.get_news():
        #     total_posts = total_posts + len(posts)
        # print('total_posts:', total_posts)
        #
        # category_nlabels = []
        # category_glabels = []
        # category_kinds = fn_dataset.get_category_kinds()
        # for category in category_kinds:
        #     category_nlabels.append(NLocalLabel(category))
        #     category_glabels.append(GlobalLabel('trending(' + category + ')'))
        #
        # nodes[0].set_node_labels(category_nlabels)
        # diff_graph.set_graph_labels(category_glabels)
        #
        # local_rules = []
        # for nlabel in category_nlabels:
        #     # local_rules.append(NetDiffLocalRule(nlabel, [], 1, [(nlabel, portion.closed(1, 1))], [], Tipping(0.5, portion.closed(1, 1))))
        #     local_rules.append(NetDiffLocalRule(nlabel, [], 1, [(nlabel, portion.closed(1, 1))], [],
        #                                         EnhancedTipping(0.5, portion.closed(1, 1))))
        #
        # global_rules = []
        # index = 0
        # for glabel in category_glabels:
        #     global_rules.append(NetDiffGlobalRule(glabel, category_nlabels[index], [], Average()))
        #     index += 1

        # predictions = PredictionsFakeNewsRob('../FakeNewsData/results/res_TextFilesCelebrity.json')

        # atom1 = Atom('news', [Variable('FN'), Variable('fake_level')])
        # atom2 = GRE(Variable('fake_level'), Constant(0.3))
        # atom3 = Atom('user', [Variable('UID')])
        # atom4 = Atom('earlyPoster', [Variable('UID'), Variable('FN')])
        # atom5 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN1')])
        # atom6 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN2')])
        # atom7 = Distinct(Variable('FN1'), Variable('FN2'))
        # atom8 = Atom('pre_hyp_fakenews', [Variable('FN')])
        # atom9 = Atom('hyp_malicious', [Variable('UID1')])
        # atom10 = Atom('hyp_malicious', [Variable('UID2')])
        # atom11 = Atom('closer', [Variable('UID1'), Variable('UID2')])
        # atom12 = Atom('pre_hyp_fakenews2', [Variable('FN')])
        # atom13 = Atom('hyp_my_resp', [Variable('FN1'), Variable('UID1')])
        # atom14 = Atom('hyp_my_resp', [Variable('FN1'), Variable('UID2')])
        # atom15 = Distinct(Variable('UID1'), Variable('UID2'))
        # atom16 = Atom('edge', [Variable('UID1'), Variable('UID2')])
        # atom17 = Atom('posted', [Variable('UID'), Variable('FN'), Variable('T')])
        # atom18 = Atom('hyp_malicious', [Variable('UID3')])
        # atom19 = Atom('closer', [Variable('UID1'), Variable('UID3')])
        # atom20 = Atom('posted', [Variable('UID1'), Variable('FN'), Variable('T')])
        # atom21 = Atom('posted', [Variable('UID2'), Variable('FN'), Variable('T')])
        # atom22 = Atom('hyp_is_resp', [Variable('UID1'), Variable('FN1')])
        # atom23 = Atom('hyp_is_resp', [Variable('UID2'), Variable('FN2')])
        # atom24 = Atom('pre_hyp_mal', [Variable('UID'), Variable('UID'), Variable('FN1'), Variable('FN2')])
        # atom25 = Atom('pre_hyp_mal', [Variable('UID1'), Variable('UID2'), Variable('FN1'), Variable('FN1')])
        # atom26 = Atom('earlyPoster', [Variable('UID1'), Variable('FN')])
        # atom27 = Atom('earlyPoster', [Variable('UID2'), Variable('FN')])
        # atom28 = Atom('pre_member', [Variable('UID1'), Variable('UID2'), Variable('FN')])
        # nct1 = NetCompTarget(atom16)

        # ont_head1 = Atom('hyp_fakenews', [Variable('FN')])
        # ont_head2 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN')])
        # ont_head3 = Atom('hyp_malicious', [Variable('UID')])
        # ont_head4 = [Atom('hyp_botnet', [Variable('B')]), Atom('member', [Variable('UID1'), Variable('B')]),
        #              Atom('member', [Variable('UID2'), Variable('B')]),
        #              Atom('member', [Variable('UID3'), Variable('B')])]
        # ont_head5 = [Atom('hyp_botnet', [Variable('B')]), Atom('member', [Variable('UID1'), Variable('B')]),
        #              Atom('member', [Variable('UID2'), Variable('B')])]
        # ont_head8 = Atom('pre_hyp_mal', [Variable('UID1'), Variable('UID2'), Variable('FN1'), Variable('FN2')])
        # ont_head9 = Atom('hyp_my_resp', [Variable('FN'), Variable('UID')])

        # ont_head5 = [Atom('hyp_botnet', [Variable('B')]), Atom('member', [Variable('UID1'), Variable('B')]),
        # atom9 = Atom('hyp_malicious', [Variable('UID1')])
        # atom10 = Atom('hyp_malicious', [Variable('UID2')])
        # atom11 = Atom('closer', [Variable('UID1'), Variable('UID2')])
        # hyp_malicious(UID1) ^ hyp_malicious(UID2) ^ closer(UID1, UID2) -> \exists B hyp_botnet(B) ^ member(UID1, B) ^ member(UID2, B)
        # tgd3 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom9, atom10, atom11], ont_head=ont_head5)

        # global_conditions = []
        # for glabel in category_glabels:
        #     # global_conditions.append((glabel, portion.closed(0.5, 1)))
        #     global_conditions.append((glabel, portion.closed(float(setting_values["trending_interval_lower"]),
        #                                                      float(setting_values["trending_interval_upper"]))))
        #
        # tgds1 = []
        # tgds2 = []
        # tgds3 = []

        # news(FN, fake_level) ^ (fake_level > \theta_1) -> hyp_fakeNews(FN) : trending(FN)
        # pre_hyp_fakenews(FN) -> hyp_fakeNews(FN) : trending(FN)
        # pre_hyp_fakenews(FN) ^ news_category(FN, C)-> hyp_fakeNews(FN) : trending(C)
        # tgd_counter = 0
        # for gc in global_conditions:
        #     # tgds.append(NetDERTGD(rule_id = 'tgd' + str(tgd_counter), ont_body = [atom8], ont_head = [ont_head1], global_cond = [gc]))
        #     news_category_atom = Atom('news_category', [Variable('FN'), Constant(category_kinds[tgd_counter])])
        #     tgds1.append(NetDERTGD(rule_id=tgd_counter, ont_body=[atom8, news_category_atom], ont_head=[ont_head1],
        #                            global_cond=[gc]))
        #     tgd_counter += 1

        # hyp_fakeNews(FN) ^ earlyPoster(UID, FN) ^ user(UID, N) -> hyp_is_resp(UID, FN)
        # hyp_fakeNews(FN) ^ earlyPoster(UID, FN) -> hyp_is_resp(UID, FN)

        # tgd1 = NetDERTGD(rule_id=1, ont_body=[ont_head1, atom4], ont_head=[ont_head2])
        # tgd_counter += 1

        # (V1) hyp_is_resp(UID, FN1) ^ hyp_is_resp(UID, FN2) ^ (FN1 != FN2) -> hyp_malicious(UID)
        # (V2) hyp_is_resp(UID, FN1) -> hyp_malicious(UID)

        # tgd2 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom5, atom6, atom7], ont_head=[ont_head3])
        # tgd2 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom22, atom23], ont_head = [ont_head8])
        # tgd_counter += 1

        # hyp_malicious(UID1) ^ hyp_malicious(UID2) ^ closer(UID1, UID2) ^ (V > \theta_2) ^ (UID1 != UID2) -> \exists B hyp_botnet(B) ^ member(UID1, B) ^ member(UID2, B)
        # tgd3 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom9, atom10, atom11], ont_head=ont_head5)
        # tgd3 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom13, atom14, atom15], ont_head = ont_head5)
        # tgd3 = NetDERTGD(rule_id = tgd_counter, ont_body = [ont_head1, atom20, atom21, atom15], ont_head = ont_head5)
        # tgd3 = NetDERTGD(rule_id = 'tgd' + str(tgd_counter), ont_body = [atom25, atom15], ont_head = ont_head5)
        # tgd3 = NetDERTGD(rule_id = tgd_counter, ont_body = [ont_head1, atom26, atom27, atom15], ont_head = ont_head5)
        # tgd3 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom28, ont_head1], ont_head = ont_head5)
        # tgd_counter += 1

        # tgd4 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom12], ont_head=[ont_head1])
        # tgd_counter += 1

        # tgd5 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom9, atom10], net_body=[nct1], ont_head=ont_head5)
        # tgd_counter += 1
        #
        # tgd6 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom5], ont_head=[ont_head3])
        # tgd_counter += 1
        #
        # tgd7 = NetDERTGD(rule_id=tgd_counter, ont_body=[ont_head1, atom17], ont_head=[ont_head2])
        # tgd_counter += 1
        #
        # tgd8 = NetDERTGD(rule_id=tgd_counter, ont_body=[atom24, atom7], ont_head=[ont_head3])
        # tgd_counter += 1
        #
        # tgds1.append(tgd4)
        # tgds2 = copy.deepcopy(tgds1)
        # tgds1.append(tgd2)
        # tgds2.append(tgd6)
        # tgds1.append(tgd1)
        # tgds2.append(tgd7)
        # tgds1.append(tgd3)
        # # tgds1.append(tgd8)
        # tgds2.append(tgd5)
        # tgds3.append(tgd1)
        # tgds3.append(tgd2)
        # tgds3.append(tgd3)
        # tgds3.append(tgd8)

        # egds = []
        # egd_counter = tgd_counter + 1
        # # hyp_botnet(B1) ^ hyp_botnet(B2) ^ member(UID, B1) ^ member(UID, B2) ->  B1 = B2
        # egd1 = NetDEREGD(rule_id=egd_counter, ont_body=[Atom('member', [Variable('UID'), Variable('B1')]),
        #                                                 Atom('member', [Variable('UID'), Variable('B2')])],
        #                  head=[Variable('B1'), Variable('B2')])
        #
        # egds.append(egd1)

        # query1 = NetDERQuery(exist_var = [], ont_cond = [Atom('hyp_fakenews', [Variable('X')]), Atom('hyp_is_resp', [Variable('Y'), Variable('Z')])], time = (2, 2))
        # query1 = NetDERQuery(exist_var = [Variable('B')], ont_cond = [Atom('hyp_fakenews', [Variable('X')]), Atom('hyp_is_resp', [Variable('Y'), Variable('Z')]), Atom('hyp_malicious', [Variable('M')]), Atom('member', [Variable('UID1'), Variable('B')]), Atom('member', [Variable('UID2'), Variable('B')]), Distinct(Variable('UID1'), Variable('UID2'))], time = (2, 2))
        # query1 = NetDERQuery(exist_var=[], ont_cond=[Atom('hyp_fakenews', [Variable('A')])],
        #                      time=(t_max, t_max))
        # query1 = NetDERQuery(exist_var = [], ont_cond = [Atom('hyp_fakenews', [Variable('X')])], time = (2, 2))
        # query1 = NetDERQuery(exist_var = [Variable('B')], ont_cond = [Atom('member', [Variable('UID1'), Variable('B')]), Atom('member', [Variable('UID2'), Variable('B')]), Distinct(Variable('UID1'), Variable('UID2'))], time = (2, 2))

        # news_dataset_ground_truth = fn_dataset.get_ground_truth()

        # orig_news_ground_truth_full = []
        # total_fn_time = []
        # for index in range(time_sim):
        #     total_fn_time.append(0)

        # index = 0
        # for posts_in_time in posts_db.get_news():
        #     news_ground_truth = {}
        #     for news in posts_in_time:
        #         found = False
        #         for time_gt in orig_news_ground_truth_full:
        #             if news in time_gt.keys():
        #                 found = True
        #                 break
        #         if not found:
        #             news_ground_truth[news] = news_dataset_ground_truth[news]
        #             if news_ground_truth[news]:
        #                 total_fn_time[index] += 1
        #     orig_news_ground_truth_full.append(news_ground_truth)
        #
        #     index += 1
        #
        # orig_news1_ground_truth_full = copy.deepcopy(orig_news_ground_truth_full)
        # orig_news2_ground_truth_full = copy.deepcopy(orig_news_ground_truth_full)

        # index = 0
        # malicious = posts_db.get_malicious()
        # orig_hyp_is_resp_gt_time = []
        # hyp_is_resp_gt_full = {}
        # time_log = {'start': None, 'end': None}
        # hyp_is_resp_time = {}
        # detection_times.append({'name': 'hyp_is_resp_time', 'value': hyp_is_resp_time})
        # for hist_pub in posts_db.get_hist_publications():
        #     hyp_is_resp_gt = {}
        #     for node in hist_pub.keys():
        #         label = False
        #         if node in malicious and news_dataset_ground_truth[hist_pub[node]]:
        #             label = True
        #         key = '(' + str(node) + ',' + str(hist_pub[node]) + ')'
        #         if not key in hyp_is_resp_gt_full.keys():
        #             hyp_is_resp_gt[key] = label
        #             if label:
        #                 clone_time_log = copy.deepcopy(time_log)
        #                 clone_time_log['start'] = index
        #                 hyp_is_resp_time[key] = clone_time_log
        #
        #             hyp_is_resp_gt_full[key] = label
        #
        #     orig_hyp_is_resp_gt_time.append(hyp_is_resp_gt)
        #     index += 1

        # orig_hyp_is_mal_gt_time = []
        # hyp_is_mal_gt_full = {}
        # hyp_is_mal_time = {}
        # detection_times.append({'name': 'hyp_is_mal_time', 'value': hyp_is_mal_time})
        # malicious = posts_db.get_malicious()
        # index = 0
        # for hist_pub in posts_db.get_hist_publications():
        #     hyp_is_mal_gt = {}
        #     for user in hist_pub.keys():
        #         label = False
        #         if user in malicious:
        #             label = True
        #         if not user in hyp_is_mal_gt_full.keys():
        #             hyp_is_mal_gt[user] = label
        #             if label:
        #                 clone_time_log = copy.deepcopy(time_log)
        #                 clone_time_log['start'] = index
        #                 hyp_is_mal_time[user] = clone_time_log
        #         hyp_is_mal_gt_full[user] = label
        #     orig_hyp_is_mal_gt_time.append(hyp_is_mal_gt)
        #     index += 1

        # orig_hyp_botnet_member_gt_time = []
        # hyp_botnet_member_gt_full = {}
        # hyp_botnet_member_time = {}
        # detection_times.append({'name': 'hyp_botnet_member_time', 'value': hyp_botnet_member_time})
        # node_index = 0
        # total_users = posts_db.get_nodes_id()
        # for node_index in range(len(total_users)):
        #     hyp_botnet_member_gt_full[str(total_users[node_index])] = False
        #
        # botnets_fn = []
        #
        # for bn in posts_db.get_botnets():
        #     for news in bn.get_posts():
        #         if not news is None and news_dataset_ground_truth[news]:
        #             botnets_fn.append(news)
        #
        # botnets = posts_db.get_members_in_botnets()
        # index = 0

        # for hist_pub in posts_db.get_hist_publications():
        #     hyp_botnet_member_gt = {}
        #     # users1 = copy.deepcopy(hist_pub.keys())
        #     # users2 = copy.deepcopy(hist_pub.keys())
        #     users = list(hist_pub.keys())
        #     for node_index in range(len(users)):
        #         label = False
        #         for botnet in botnets:
        #
        #             if users[node_index] in copy.deepcopy(botnet):
        #                 label = True
        #                 break
        #         found = False
        #         for time_gt in orig_hyp_botnet_member_gt_time:
        #             if str(users[node_index]) in time_gt.keys():
        #                 found = True
        #                 break
        #         if not found:
        #             hyp_botnet_member_gt[str(users[node_index])] = label
        #             if label:
        #                 clone_time_log = copy.deepcopy(time_log)
        #                 clone_time_log['start'] = index
        #                 hyp_botnet_member_time[str(users[node_index])] = clone_time_log
        #         hyp_botnet_member_gt_full[str(users[node_index])] = label
        #     orig_hyp_botnet_member_gt_time.append(hyp_botnet_member_gt)
        #     index += 1
        #
        # total_botnets_fn += len(botnets_fn)

        # earlyPoster = EarlyPoster(posts_db)
        # preMember = PreMember(earlyPoster)
        # closer = Closer(posts_db)
        # news_category = NewsCategory(posts_db)

        # atoms = []
        # facts = []
        # kbs = []
        # gt_fn_atoms = []
        #
        # kbs.append(NetDERKB(ont_data=atoms, net_db=NetDB(diff_graph, facts), netder_tgds=tgds1, netder_egds=egds,
        #                     netdiff_lrules=local_rules, netdiff_grules=global_rules))
        # kbs.append(NetDERKB(ont_data=atoms, net_db=NetDB(diff_graph, facts), netder_tgds=tgds2, netder_egds=egds,
        #                     netdiff_lrules=local_rules, netdiff_grules=global_rules))
        # kbs.append(NetDERKB(ont_data=[], net_db=NetDB(diff_graph, facts), netder_tgds=tgds3, netder_egds=egds,
        #                     netdiff_lrules=local_rules, netdiff_grules=global_rules))
        # for i in range(len(kbs)):
        #     gt_fn_atoms.append([])
        #     for j in range(time_sim):
        #         gt_fn_atoms[i].append([])
        #
        # for i in range(len(orig_news_ground_truth_full)):
        #     for key in orig_news_ground_truth_full[i].keys():
        #         if orig_news_ground_truth_full[i][key]:
        #             gt_fn_atoms[len(gt_fn_atoms) - 1][i].append(Atom('hyp_fakenews', [Constant(str(key))]))
        #
        # for kb_index in range(len(kbs)):
        #     news_ground_truth_full = copy.deepcopy(orig_news_ground_truth_full)
        #     news1_ground_truth_full = copy.deepcopy(orig_news1_ground_truth_full)
        #     news2_ground_truth_full = copy.deepcopy(orig_news2_ground_truth_full)
        #     hyp_is_resp_gt_time = copy.deepcopy(orig_hyp_is_resp_gt_time)
        #     hyp_is_mal_gt_time = copy.deepcopy(orig_hyp_is_mal_gt_time)
        #     hyp_botnet_member_gt_time = copy.deepcopy(orig_hyp_botnet_member_gt_time)
        #     kb = kbs[kb_index]
        #     chase = NetDERChase(kb, t_max)
        #     answers_hyp_fakenews = []
        #     answers_hyp_fakenews1 = []
        #     answers_hyp_fakenews2 = []
        #     answers_hyp_is_resp = []
        #     answers_hyp_malicious = []
        #     answers_hyp_member = []
        #     reduced_result_eval = {}
        #     botnets_fn_det = 0
        #     fn_tps = 0
        #     for item in ['fnA', 'fnB', 'fnC', 'resp', 'mal', 'memb']:
        #         reduced_result_eval[item] = []
        #
        #     for time in range(time_sim):
        #         print('time:', time)
        #         answers_hyp_fakenews.append([])
        #         answers_hyp_fakenews1.append([])
        #         answers_hyp_fakenews2.append([])
        #         answers_hyp_is_resp.append([])
        #         answers_hyp_malicious.append([])
        #         answers_hyp_member.append([])
        #         result_eval["time_sim"] = time
        #         result_eval['fn'] = total_fn_time[time]
        #         result_eval['bn_fn'] = None
        #         result_eval['bn_fn_det'] = None
        #         result_eval['bn_fn_det2'] = None

        # atoms = atoms + ont_db[time]
        # atoms = atoms + earlyPoster.get_atoms()


#                 atoms = gt_fn_atoms[kb_index][time] + ont_db[time] + earlyPoster.get_atoms(time) + closer.get_atoms(
#                     time) + news_category.get_atoms(time)
#                 # atoms = ont_db[time] + earlyPoster.get_atoms()
#                 print('len(new_atoms):', len(atoms))
#
#                 print('antes de remover pre_hyp_fakenews len(ont_db):', kb.get_ont_db().get_size())
#
#                 # kb.remove_atoms_from_pred('pre_hyp_fakenews')
#                 print('despues de remover pre_hyp_fakenews len(ont_db):', kb.get_ont_db().get_size())
#                 kb.add_ont_knowledge(atoms)
#
#                 print('despues de agregar new atoms len(ont_db):', kb.get_ont_db().get_size())
#                 # facts = facts + net_db[time]
#                 facts = net_db[time]
#                 kb_net_db = kb.get_net_db()
#                 kb_net_db.set_facts(facts)
#                 # kb = NetDERKB(ont_data = atoms, net_db = NetDB(diff_graph, facts), netder_tgds = tgds, netder_egds = egds, netdiff_lrules = local_rules, netdiff_grules = global_rules)
#                 chase = NetDERChase(kb, t_max)
#
#                 inicio_q1 = datetime.now()
#                 answers = chase.answer_query(query1, 1)
#                 fin_q1 = datetime.now()
#
#                 if not answers is None:
#                     print('len answers:', len(answers))
#                     for answer in answers:
#                         contador = 0
#                         if 'X' in answer.keys():
#                             value = answer['X'].getValue()
#                             found = False
#                             for t in range(time):
#                                 if value in answers_hyp_fakenews[t]:
#                                     found = True
#                                     break
#                             if not found:
#                                 answers_hyp_fakenews[time].append(value)
#                                 if news_dataset_ground_truth[value]:
#                                     fn_tps += 1
#                                 if value in botnets_fn:
#                                     botnets_fn_det += 1
#
#                                 if not value in news_ground_truth_full[time].keys():
#                                     news_ground_truth_full[time][value] = news_dataset_ground_truth[value]
#
#                         if 'Y' in answer.keys() and 'Z' in answer.keys():
#                             value = '(' + str(answer['Y'].getValue()) + ',' + str(answer['Z'].getValue()) + ')'
#                             found = False
#                             for t in range(time):
#                                 if value in answers_hyp_is_resp[t]:
#                                     found = True
#                                     break
#                             if not found:
#                                 answers_hyp_is_resp[time].append(value)
#                                 if value in hyp_is_resp_time.keys():
#                                     hyp_is_resp_time[value]['end'] = time
#
#                                 if not value in hyp_is_resp_gt_time[time].keys():
#                                     hyp_is_resp_gt_time[time][value] = hyp_is_resp_gt_full[value]
#
#                         if 'M' in answer.keys():
#                             value = str(answer['M'].getValue())
#                             found = False
#                             for t in range(time):
#                                 if value in answers_hyp_malicious[t]:
#                                     found = True
#                                     break
#                             if not found:
#                                 answers_hyp_malicious[time].append(value)
#                                 if value in hyp_is_mal_time.keys():
#                                     hyp_is_mal_time[value]['end'] = time
#                                 if not value in hyp_is_mal_gt_time[time].keys():
#                                     hyp_is_mal_gt_time[time][value] = hyp_is_mal_gt_full[value]
#
#                         if 'UID1' in answer.keys():
#                             value = str(answer['UID1'].getValue())
#                             found = False
#                             for t in range(time):
#                                 if value in answers_hyp_member[t]:
#                                     found = True
#                                     break
#                             if not found:
#                                 if value in hyp_botnet_member_time.keys():
#                                     hyp_botnet_member_time[value]['end'] = time
#                                 answers_hyp_member[time].append(value)
#                                 if not value in hyp_botnet_member_gt_time[time].keys():
#                                     hyp_botnet_member_gt_time[time][value] = hyp_botnet_member_gt_full[value]
#
#                 print('len(answers_hyp_fakenews)')
#                 print(len(answers_hyp_fakenews[time]))
#                 print('len pre_hyp_fakenews1')
#                 for pre_hyp_fn in kb.get_ont_db().get_atoms_from_pred('pre_hyp_fakenews'):
#                     value = pre_hyp_fn.get_terms()[0].getValue()
#                     found = False
#                     for t in range(time):
#                         if value in answers_hyp_fakenews1[t]:
#                             found = True
#                             break
#                     if not found:
#                         answers_hyp_fakenews1[time].append(value)
#                         if not value in news1_ground_truth_full[time].keys():
#                             news1_ground_truth_full[time][value] = news_dataset_ground_truth[value]
#
#                 print(len(answers_hyp_fakenews1[time]))
#
#                 print('len pre_hyp_fakenews2')
#                 for pre_hyp_fn in kb.get_ont_db().get_atoms_from_pred('pre_hyp_fakenews2'):
#                     value = pre_hyp_fn.get_terms()[0].getValue()
#                     found = False
#                     for t in range(time):
#                         if value in answers_hyp_fakenews2[t]:
#                             found = True
#                             break
#                     if not found:
#                         answers_hyp_fakenews2[time].append(value)
#                         if not value in news2_ground_truth_full[time].keys():
#                             news2_ground_truth_full[time][value] = news_dataset_ground_truth[value]
#
#                 print(len(answers_hyp_fakenews2[time]))
#
#                 result_eval["fnA"] = len(answers_hyp_fakenews[time])
#                 print('len(answers_hyp_is_resp)')
#                 print(len(answers_hyp_is_resp[time]))
#                 result_eval["resp"] = len(answers_hyp_is_resp[time])
#                 print('len(answers_hyp_malicious)')
#                 print(len(answers_hyp_malicious[time]))
#                 result_eval["mal"] = len(answers_hyp_malicious[time])
#                 print('len(answers_hyp_member)')
#                 print(len(answers_hyp_member[time]))
#                 result_eval["memb"] = len(answers_hyp_member[time])
#
#                 print('len(news_ground_truth_full[time])', len(news_ground_truth_full[time]))
#                 print('answers_hyp_fakenews', len(answers_hyp_fakenews[time]))
#                 evaluator1 = Evaluator(news_ground_truth_full[time], answers_hyp_fakenews[time])
#                 result_eval1 = evaluator1.evaluate()
#                 result_eval["fnA_prec"] = None
#                 result_eval["fnA_rec"] = None
#
#                 reduced_result_eval["fnA"].append(result_eval1)
#                 result_eval["fnA_tp"] = result_eval1["tp"]
#                 result_eval["fnA_fp"] = result_eval1["fp"]
#                 result_eval["fnA_tn"] = result_eval1["tn"]
#                 result_eval["fnA_fn"] = result_eval1["fn"]
#                 evaluator2 = Evaluator(hyp_is_resp_gt_time[time], answers_hyp_is_resp[time])
#                 result_eval2 = evaluator2.evaluate()
#                 result_eval["resp_prec"] = None
#                 result_eval["resp_rec"] = None
#
#                 reduced_result_eval["resp"].append(result_eval2)
#                 result_eval["resp_tp"] = result_eval2["tp"]
#                 result_eval["resp_fp"] = result_eval2["fp"]
#                 result_eval["resp_tn"] = result_eval2["tn"]
#                 result_eval["resp_fn"] = result_eval2["fn"]
#                 evaluator3 = Evaluator(hyp_is_mal_gt_time[time], answers_hyp_malicious[time])
#                 result_eval3 = evaluator3.evaluate()
#                 result_eval["mal_prec"] = None
#                 result_eval["mal_rec"] = None
#
#                 reduced_result_eval["mal"].append(result_eval3)
#                 result_eval["mal_tp"] = result_eval3["tp"]
#                 result_eval["mal_fp"] = result_eval3["fp"]
#                 result_eval["mal_tn"] = result_eval3["tn"]
#                 result_eval["mal_fn"] = result_eval3["fn"]
#                 evaluator4 = Evaluator(hyp_botnet_member_gt_time[time], answers_hyp_member[time])
#                 result_eval4 = evaluator4.evaluate()
#                 result_eval["memb_prec"] = None
#                 result_eval["memb_rec"] = None
#
#                 reduced_result_eval["memb"].append(result_eval4)
#                 result_eval["memb_tp"] = result_eval4["tp"]
#                 result_eval["memb_fp"] = result_eval4["fp"]
#                 result_eval["memb_tn"] = result_eval4["tn"]
#                 result_eval["memb_fn"] = result_eval4["fn"]
#                 evaluator5 = Evaluator(news1_ground_truth_full[time], answers_hyp_fakenews1[time])
#                 result_eval5 = evaluator5.evaluate()
#                 result_eval["fnB_prec"] = None
#                 result_eval["fnB_rec"] = None
#
#                 reduced_result_eval["fnB"].append(result_eval5)
#                 result_eval["fnB_tp"] = result_eval5["tp"]
#                 result_eval["fnB_fp"] = result_eval5["fp"]
#                 result_eval["fnB_tn"] = result_eval5["tn"]
#                 result_eval["fnB_fn"] = result_eval5["fn"]
#                 evaluator6 = Evaluator(news2_ground_truth_full[time], answers_hyp_fakenews2[time])
#                 result_eval6 = evaluator6.evaluate()
#                 result_eval["fnC_prec"] = None
#                 result_eval["fnC_rec"] = None
#
#                 reduced_result_eval["fnC"].append(result_eval6)
#                 result_eval["fnC_tp"] = result_eval6["tp"]
#                 result_eval["fnC_fp"] = result_eval6["fp"]
#                 result_eval["fnC_tn"] = result_eval6["tn"]
#                 result_eval["fnC_fn"] = result_eval6["fn"]
#
#                 print("Evaluacion hyp_fakenews:", result_eval1)
#                 print("Evaluacion pre_fake_news1:", result_eval5)
#                 print("Evaluacion pre_fake_news2:", result_eval6)
#                 print("Evaluacion hyp_is_resp:", result_eval2)
#                 print("Evaluacion hyp_is_malicious:", result_eval3)
#                 print("Evaluacion hyp_members:", result_eval4)
#
#                 print('Tiempo chase consulta 1:', fin_q1 - inicio_q1)
#
#                 result_eval["query_time"] = fin_q1 - inicio_q1
#
#                 if time == (time_sim - 1):
#                     result_eval['bn_fn'] = len(botnets_fn)
#                     if len(botnets_fn) != 0:
#                         result_eval['bn_fn_det'] = botnets_fn_det / len(botnets_fn)
#                         total_botnets_fn_det[kb_index] += result_eval['bn_fn_det']
#                     if fn_tps != 0:
#                         result_eval['bn_fn_det2'] = botnets_fn_det / fn_tps
#                         total_botnets_fn_det2[kb_index] += result_eval['bn_fn_det2']
#                     for key in reduced_result_eval.keys():
#                         tp = 0
#                         fp = 0
#                         fn = 0
#                         prec = None
#                         rec = None
#                         f1 = None
#                         for evaluator in reduced_result_eval[key]:
#                             tp += evaluator['tp']
#                             fp += evaluator['fp']
#                             fn += evaluator['fn']
#
#                         if (fp + tp) != 0:
#                             prec = tp / (fp + tp)
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'prec']['total_value'] += prec
#                             result_eval[str(key) + '_' + 'prec'] = prec
#                         else:
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'prec']['total_samples'] -= 1
#
#                         if (fn + tp) != 0:
#                             rec = tp / (fn + tp)
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'rec']['total_value'] += rec
#                             result_eval[str(key) + '_' + 'rec'] = rec
#                         else:
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'rec']['total_samples'] -= 1
#
#                         if not ((prec is None or rec is None) or (prec == 0 and rec == 0)):
#                             f1 = 2 * (prec * rec) / (prec + rec)
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'f1']['total_value'] += f1
#                         else:
#                             reduced_result_eval2[kb_index][str(key) + '_' + 'f1']['total_samples'] -= 1
#
#                 save_csv('../result_netder_experiment_sett_' + setting_values['setting'] + '_prog' + str(
#                     kb_index + 1) + '.csv', result_eval)
#
#             for detection in detection_times:
#                 writepath = '../time_detection_results/' + detection['name'] + '_sett' + setting_values[
#                     'setting'] + '_run' + str(run + 1) + '_prog' + str(kb_index + 1) + '.json'
#                 with open(writepath, 'w') as outfile:
#                     json.dump(detection['value'], outfile)
#
#             member_data = kb.get_data_from_pred('member')
#             print('len(member_data)', len(member_data))
#
#             print('Tiempo construccion DB:', fin_bdb - inicio_bdb)
#             fin_setting = datetime.now()
#             print('Tiempo setting nro', setting_values['setting'], 'run nro', run + 1, ':',
#                   fin_setting - inicio_setting)
#             setting_counter += 1
#             gt = news_ground_truth_full
#             fn = 0
#             rn = 0
#             fn_cat_counter = {}
#             rn_cat_counter = {}
#             for category in fn_dataset.get_category_kinds():
#                 fn_cat_counter[category] = 0
#                 rn_cat_counter[category] = 0
#             total = 0
#             for gt in news_ground_truth_full:
#                 total += len(gt.keys())
#                 for key in gt.keys():
#                     category = fn_dataset.get_category(key)
#                     if gt[key] == True:
#                         fn_cat_counter[category] += 1
#                         fn += 1
#                     elif gt[key] == False:
#                         rn_cat_counter[category] += 1
#                         rn += 1
#             print('setting', setting_values['setting'], 'run', run + 1)
#             if total != 0:
#                 print('porcentaje fake news', fn / total)
#                 print('porcentaje real news', rn / total)
#             print('cuenta por categoria en fn', fn_cat_counter)
#             print('cuenta por categoria en rn', rn_cat_counter)
#
#     rr_counter = 0
#     for rr in reduced_result_eval2:
#         for key in rr.keys():
#             if not rr[key] is None:
#                 prev_value = rr[key]['total_value']
#                 total_samples = rr[key]['total_samples']
#                 if total_samples > 0:
#                     rr[key] = prev_value / total_samples
#                 else:
#                     rr[key] = None
#
#         rr['prog'] = programs[rr_counter]
#         rr['sett'] = setting_values['setting']
#         rr['bn_fn'] = total_botnets_fn / int(setting_values["cant_run"])
#         rr['bn_fn_det'] = total_botnets_fn_det[rr_counter] / int(setting_values["cant_run"])
#         rr['bn_fn_det2'] = total_botnets_fn_det2[rr_counter] / int(setting_values["cant_run"])
#         save_csv('../reduced_result_experiment.csv', rr)
#         rr_counter += 1
#
#
# fin = datetime.now()
# print('Tiempo total:', fin - inicio)

if __name__ == "__main__":
    main()
    exit(0)