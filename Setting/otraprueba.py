import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import csv

'''
with open('../result_netder_experiment.csv', 'r', newline='') as csvfile:
	#fieldnames = ["setting", "run", "query_time", "time_sim", "hyp_fakenews", "hyp_fn_precision", "hyp_fn_recall", "hyp_fn1_precision", "hyp_fn1_recall", "hyp_fn2_precision", "hyp_fn2_recall", "hyp_fakenews_tp", "hyp_fakenews_fp", "hyp_fakenews_tn", "hyp_fakenews_fn", "hyp_is_resp", "h_resp_precision", "h_resp_recall", "hyp_is_resp_tp", "hyp_is_resp_fp", "hyp_is_resp_tn", "hyp_is_resp_fn", "hyp_is_malicious", "h_mal_precision", "h_mal_recall", "hyp_is_malicious_tp", "hyp_is_malicious_fp", "hyp_is_malicious_tn", "hyp_is_malicious_fn", "hyp_members", "h_memb_precision", "h_memb_recall", "hyp_members_tp", "hyp_members_fp", "hyp_members_tn", "hyp_members_fn"]
	reader = csv.DictReader(csvfile)
	headers = reader.fieldnames

print(headers)'''

with open('../test.json', 'w') as outfile:
	pass
