# from typing import Dict, Any, Union, Tuple
#

class EvaluatorTesis:
    fp = 0
    vp = 0
    vn = 0
    fn = 0
    old_malicious_accounts = set()
    old_non_malicious_accounts = set()
    account_aparitions = {}
    malicious_account_distance = {}
    metrics_result = {}

    def __init__(self, df_ground_truth):
        self._set_ground_truth = set(df_ground_truth['address'].unique())

    def evaluate(self, malicious_accounts, accounts_in_sd, sd):
        malicious_sd = set(ans['A1'].getValue() for ans in malicious_accounts)
        new_non_malicious_accounts = set(accounts_in_sd['2_address']).difference(malicious_sd).difference(self.old_non_malicious_accounts)
        new_malicious_accounts = malicious_sd.difference(self.old_malicious_accounts)
        print('NetDer ANS: ' + str([s for s in malicious_sd]))
        print('Accounts: ' + str([s for s in accounts_in_sd['2_address']]))
        print('new_non_malicious_accounts: ' + str([s for s in new_non_malicious_accounts]))
        print('new_malicious_accounts: ' + str([s for s in new_malicious_accounts]))

        for new_acc in new_non_malicious_accounts:
            if new_acc in self._set_ground_truth: # Solo me interesa calcular distnacias de cuentas en el ground truth
                self.account_aparitions[new_acc] = sd

        for new_mal_acc in new_malicious_accounts:
            if new_mal_acc in self._set_ground_truth:
                if new_mal_acc in self.account_aparitions.keys():
                    self.malicious_account_distance[new_mal_acc] = sd - self.account_aparitions[new_mal_acc]
                else:
                    self.malicious_account_distance[new_mal_acc] = 0

        print('Distancias')
        print(self.malicious_account_distance)

        for mal_account in new_malicious_accounts:
            if mal_account in self._set_ground_truth:
                self.vp += 1
            else:
                self.fp += 1
        for account in new_non_malicious_accounts:
            if account in self._set_ground_truth:
                self.fn += 1
            else:
                self.vn += 1

        self.old_malicious_accounts = self.old_malicious_accounts.union(new_malicious_accounts)
        self.old_non_malicious_accounts = self.old_non_malicious_accounts.union(new_non_malicious_accounts)


        f1 = None
        if (self.vp + self.fp) != 0:
            precision = self.vp / (self.vp + self.fp)
        else:
            precision = None
        if (self.vp + self.fn) != 0:
            recall = self.vp / (self.vp + self.fn)
        else:
            recall = None
        if (not precision is None) and (not recall is None):
            if precision != 0 or recall != 0:
                f1 = 2 * (precision * recall) / (precision + recall)

        self.metrics_result = {"fn": self.fn, "tn": self.vn, "tp": self.vp, "fp": self.fp, "precision": precision, "recall": recall, 'f1': f1}
        return self.metrics_result

    def save_results(self, corrida):
      file_metrics = open('./results/metrics.csv','w')
      file_metrics.write(corrida+",fn,tn,tp,fp,precision,recall,f1")
      result_metrics = str(corrida+','+self.metrics_result['fn'])+','+str(self.metrics_result['tn'])+','+str(self.metrics_result['tp'])+','+str(self.metrics_result['fp'])+','\
               +str(self.metrics_result['precision'])+','+str(self.metrics_result['recall'])+','+str(self.metrics_result['f1'])
      file_metrics.write(result_metrics)

      file_metrics = open('./results/distances.csv', 'w')
      file_metrics.write(corrida + ",account,sd")
      result_metrics = str(corrida + ',' + self.metrics_result['fn']) + ',' + str(
          self.metrics_result['tn']) + ',' + str(self.metrics_result['tp']) + ',' + str(self.metrics_result['fp']) + ',' \
                       + str(self.metrics_result['precision']) + ',' + str(self.metrics_result['recall']) + ',' + str(
          self.metrics_result['f1'])
      file_metrics.write(result_metrics)