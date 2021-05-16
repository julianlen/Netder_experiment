import os, sys

from Diffusion_Process.NetDiffProgram import NetDiffProgram

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import copy
from Ontological.Variable import Variable
from Ontological.Null import Null
from Ontological.OntDB import OntDB
from Ontological.Homomorphism import Homomorphism
from Diffusion_Process.NetDiffInterpretation import NetDiffInterpretation


class NetDERChase:

    def __init__(self, kb, tmax=1):
        self._kb = kb
        self._tmax = tmax
        self._net_diff_interpretation = NetDiffInterpretation(self._kb.get_net_diff_graph(), self._tmax)
        self._body_mapping_his = []
        self._rule_map_his = {}

    def _get_atoms_mapping(self, atoms, data_base):
        h = Homomorphism()
        return h.get_atoms_mapping(atoms, data_base) # Genera el homomorfismo entre los atomos de atoms y todos en data_base

    def _get_candidate_atoms(self, rule):
        candidates = []
        atom_ids = set()
        for atom in rule.get_ont_body():
            if not atom.getId() in atom_ids:
                print(atom.getId())
                candidates = candidates + self._kb.get_data_from_pred(atom.getId())
                atom_ids.add(atom.getId())

        for nct in rule.get_net_body():
            if not nct.getComponent().getId() in atom_ids:
                candidates = candidates + self._kb.get_data_from_pred(nct.getComponent().getId())
                atom_ids.add(nct.getComponent().getId())

        return candidates

    def _search_body_mapping(self, atoms):
        ont_bd = OntDB(atoms)
        result = None

        for bm in self._body_mapping_his:
            if ont_bd.is_equivalent(bm[0]):
                result = bm[1]
                break

        return result

    def get_body_mapping(self, rule, time):
        # net_db = []
        # cloned_net_body = copy.deepcopy(rule.get_net_body())
        # for nct in cloned_net_body:
        #     net_db.append(nct.getComponent())

        # body_mapping = self._search_body_mapping(rule.get_ont_body() + net_db)
        body_mapping = self._search_body_mapping(rule.get_ont_body())

        if body_mapping is None:
            body_mapping = self._get_atoms_mapping(rule.get_ont_body(), self._get_candidate_atoms(rule)) # Obtiene los atomos candidatos para mapear el cuerpo
            # body_mapping = self._get_atoms_mapping(rule.get_ont_body() + net_db, self._get_candidate_atoms(rule))
            if len(body_mapping) > 0:
                # ont_db = OntDB(rule.get_ont_body() + net_db)
                ont_db = OntDB(rule.get_ont_body())
                self._body_mapping_his.append((ont_db, body_mapping))

        aux_body_mapping = {}
        if len(body_mapping) > 0:
            for key in body_mapping.keys():
                # if len(time) > 0:
                #     net_db = []
                #     cloned_net_body = copy.deepcopy(rule.get_net_body())
                #     for nct in cloned_net_body:
                #         net_db.append(nct.getComponent())
                #     for atom in net_db:
                #         atom.map(body_mapping[key])
                #     if len(cloned_net_body) > 0:
                #         net_db = []
                #         cloned_net_body = copy.deepcopy(rule.get_net_body())
                #         for nct in cloned_net_body:
                #             net_db.append(nct.getComponent())
                #         for atom in net_db:
                #             atom.map(body_mapping[key])
                #         for nct in cloned_net_body:
                #             comp = self._kb.get_comp_from_atom(nct.getComponent())
                #             for t in range(time[0], time[1] + 1):
                #                 if self._net_diff_interpretation.isSatisfied(t, comp, (nct.getLabel(), nct.getBound())):
                #                     if self._net_diff_interpretation.areSatisfied(t, self._kb.get_net_diff_graph(),
                #                                                                   rule.get_global_cond()):
                #                         aux_body_mapping[key] = body_mapping[key]
                #
                #     else:
                #         for t in range(time[0], time[1] + 1):
                #             if self._net_diff_interpretation.areSatisfied(t, self._kb.get_net_diff_graph(),
                #                                                           rule.get_global_cond()):
                #                 aux_body_mapping[key] = body_mapping[key]
                #
                # else:
                aux_body_mapping = body_mapping
        body_mapping = aux_body_mapping

        return body_mapping

    def _rem_known_mappings(self, id_rule, mapping):
        result = {}
        if len(mapping) > 0:
            if not (id_rule in self._rule_map_his):
                self._rule_map_his[id_rule] = set()

            mapping_his = self._rule_map_his[id_rule]

            for key_pos in mapping.keys():
                if not (key_pos in mapping_his):
                    result[key_pos] = mapping[key_pos]
                    mapping_his.add(key_pos)

        return result

    def applyStepTGDChase(self, tgd, time):
        body_mapping = self.get_body_mapping(tgd, time)
        ont_head_result = []
        net_head_result = []
        if len(body_mapping) > 0:
            for key_pos in body_mapping.keys():
                net_head_comp = []
                cloned_net_head = copy.deepcopy(tgd.get_net_head())
                for nct in cloned_net_head:
                    net_head_comp.append(nct.getComponent())
                cloned_ont_head = copy.deepcopy(tgd.get_ont_head())

                for atom in cloned_ont_head:
                    atom.map(body_mapping[key_pos])
                for comp in net_head_comp:
                    comp.map(body_mapping[key_pos])
                ont_head_result.append(cloned_ont_head)
                net_head_result.append(cloned_net_head)
        aux_result = [[], []]
        index = 0
        for possibility in ont_head_result:
            for atom in possibility:
                for term in atom.get_terms():
                    if isinstance(term, Variable):
                        null = Null()
                        for _atom in possibility:
                            _atom.map({term.getId(): null})
                        for nct in net_head_result[index]:
                            nct.getComponent().map({term.getId(): null})
            aux_result[0] = aux_result[0] + possibility

        for possibility in net_head_result:
            for nct in possibility:
                for term in nct.getComponent().get_terms():
                    if isinstance(term, Variable):
                        null = Null()
                        for nct in possibility:
                            nct.getComponent().map({term.getId(): null})
            aux_result[1] = aux_result[1] + possibility

        return aux_result

    def applyStepEGDChase(self, egd, time):
        success = True
        # se busca si existe algun mapeo del cuerpo
        body_mapping = self.get_body_mapping(egd, time)

        new_mapping = {}
        if len(body_mapping) > 0:
            for key_pos in body_mapping.keys():
                head = copy.deepcopy(egd.get_head())
                cloned_ont_body1 = copy.deepcopy(egd.get_ont_body())
                cloned_net_body1 = copy.deepcopy(egd.get_net_body())
                cloned_ont_body2 = copy.deepcopy(egd.get_ont_body())
                cloned_net_body2 = copy.deepcopy(egd.get_net_body())
                for atom in cloned_ont_body1:
                    atom.map({head[0].getId(): head[1]})
                    atom.map(body_mapping[key_pos])

                for nct in cloned_net_body1:
                    nct.getComponent().map({head[0].getId(): head[1]})
                    nct.getComponent().map(body_mapping[key_pos])

                head = copy.deepcopy(egd.get_head())
                for atom in cloned_ont_body2:
                    atom.map({head[1].getId(): head[0]})
                    atom.map(body_mapping[key_pos])

                for nct in cloned_net_body2:
                    nct.getComponent().map({head[1].getId(): head[0]})
                    nct.getComponent().map(body_mapping[key_pos])

                for index in range(0, len(cloned_ont_body1)):
                    term_i = 0
                    for term in cloned_ont_body1[index].get_terms():
                        if term.can_be_instanced():
                            new_mapping[term.getId()] = cloned_ont_body2[index].get_terms()[term_i]
                        elif cloned_ont_body2[index].get_terms()[term_i].can_be_instanced():
                            new_mapping[cloned_ont_body2[index].get_terms()[term_i].getId()] = term
                        elif not term.getValue() == cloned_ont_body2[index].get_terms()[term_i].getValue():
                            success = False
                            break
                        term_i = term_i + 1
                for index in range(0, len(cloned_net_body1)):
                    term_i = 0
                    for term in cloned_net_body1[index].getComponent().get_terms():
                        if term.can_be_instanced():
                            new_mapping[term.getId()] = cloned_net_body2[index].getComponent().get_terms()[term_i]
                        elif cloned_net_body2[index].getComponent().get_terms()[term_i].can_be_instanced():
                            new_mapping[cloned_net_body2[index].getComponent().get_terms()[term_i].getId()] = term
                        elif (
                                not term.getValue() == cloned_net_body2[index].getComponent().get_terms()[
                                    term_i].getValue()):
                            success = False
                            break
                        term_i = term_i + 1
            if success:
                self._kb.apply_map(new_mapping)

        return success

    def answer_query(self, query, int_bound):
        # result = []
        done = False
        counter = 0
        mapping = {}
        self._net_diff_interpretation = NetDiffInterpretation(self._kb.get_net_diff_graph(), self._tmax)
        while (counter <= int_bound) and not done:
            mapping = {}
            while not done:
                # new_knowledge = [ont_knowledge, net_knowledge]
                new_knowledge, done, success = self.execute_chase(query)
                self._body_mapping_his = []
                if not done:
                    done, mapping = self.evaluate_query(done, mapping, new_knowledge, query, success)
                else:
                    mapping = {}
            self.diffusion_process()
            done = False
            counter = counter + 1
            # print('final net_diff_interpretation')
            # print(str(self._net_diff_interpretation))
        #  Answer construction
        return self.answer_construction(mapping, query)

    def answer_construction(self, mapping, query):
        result = []
        for key_pos in mapping.keys():
            aux_result_mapping = {}
            for key in mapping[key_pos].keys():
                if not (Variable(key) in query.get_exist_var()):
                    aux_result_mapping[key] = mapping[key_pos][key]
            if len(aux_result_mapping) > 0:
                result.append(aux_result_mapping)
        return result

    def diffusion_process(self):
        net_diff_program = NetDiffProgram(self._kb.get_net_diff_graph(), self._tmax,
                                          self._kb.get_net_diff_facts(), self._kb.get_net_diff_lrules(),
                                          self._kb.get_net_diff_grules())
        self._net_diff_interpretation = net_diff_program.diffusion()

    def evaluate_query(self, done, mapping, new_knowledge, query, success):
        query_answer_success = True
        for q in query.get_disjoint_queries():
            candidates = self._get_candidate_atoms(q)
            q_mapping = self._get_atoms_mapping(q.get_ont_body(), candidates)
            mapping.update(q_mapping)
            if not (len(q_mapping) > 0):
                query_answer_success = False
        if query_answer_success or ((not success) and (len(new_knowledge[1]) == 0)):
            done = True
        return done, mapping

    def execute_chase(self, query):
        done = False
        new_knowledge = [[], []]
        for tgd in self._kb.get_netder_tgds():
            TGD_result = self.applyStepTGDChase(tgd, query.get_time())
            new_knowledge[0] = new_knowledge[0] + TGD_result[0]  # D
            new_knowledge[1] = new_knowledge[1] + TGD_result[1]  # G
        success = self._kb.add_ont_knowledge(new_knowledge[0])
        self._kb.add_net_knowledge(new_knowledge[1], query.get_time())
        for egd in self._kb.get_netder_egds():
            done = self.applyStepEGDChase(egd, query.get_time())
            print('seguir egd')
            print(done)
            if done:
                break
        return new_knowledge, done, success
