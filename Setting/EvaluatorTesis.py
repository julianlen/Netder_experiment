# from typing import Dict, Any, Union, Tuple
#

class EvaluatorTesis:
    fp = 0
    vp = 0
    vn = 0
    fn = 0
    old_malicious_accounts = set()
    old_non_malicious_accounts = set()

    def __init__(self, df_ground_truth):
        self._set_ground_truth = set(df_ground_truth['address'].unique())

    def evaluate(self, malicious_accounts, accounts_in_sd):
        malicious_sd = set(ans['A1'].getValue() for ans in malicious_accounts)
        new_non_malicious_accounts = set(accounts_in_sd['2_address']).difference(malicious_sd).difference(self.old_non_malicious_accounts)
        new_malicious_accounts = malicious_sd.difference(self.old_malicious_accounts)

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

        self.old_malicious_accounts.union(new_malicious_accounts)
        self.old_non_malicious_accounts.union(new_non_malicious_accounts)

        precision = 0
        recall = 0
        f1 = None
        if ((self.vp + self.fp) != 0):
            precision = self.vp / (self.vp + self.fp)
        else:
            precision = None
        if ((self.vp + self.fn) != 0):
            recall = self.vp / (self.vp + self.fn)
        else:
            recall = None
        if (not precision is None) and (not recall is None):
            if precision != 0 or recall != 0:
                f1 = 2 * (precision * recall) / (precision + recall)

        return {"fn": self.fn, "tn": self.vn, "tp": self.vp, "fp": self.fp, "precision": precision, "recall": recall, 'f1': f1}
