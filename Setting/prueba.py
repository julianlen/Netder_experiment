import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from datetime import datetime
import csv
import portion
import copy
import random
import string
import subprocess
from FakeNewsData.AugmentedDatasetCSVLiar import AugmentedDatasetCSVLiar
from FakeNewsData.DatasetCSVLiar import DatasetCSVLiar
from FakeNewsData.AugmentedDatasetTextFilesCelebrity import AugmentedDatasetTextFilesCelebrity
from AFPostDatabase import AFPostDatabase
from EarlyPoster import EarlyPoster
from Closer import Closer
from Evaluator import Evaluator
from Diffusion_Process.NetDiffNode import NetDiffNode
from Diffusion_Process.NetDiffEdge import NetDiffEdge
from Diffusion_Process.NetDiffGraph import NetDiffGraph
from Diffusion_Process.NetDiffFact import NetDiffFact
from Diffusion_Process.NLocalLabel import NLocalLabel
from Diffusion_Process.GlobalLabel import GlobalLabel
from Diffusion_Process.NetDiffLocalRule import NetDiffLocalRule
from Diffusion_Process.NetDiffGlobalRule import NetDiffGlobalRule
from Diffusion_Process.Average import Average
from Diffusion_Process.Tipping import Tipping
from Diffusion_Process.EnhancedTipping import EnhancedTipping
from Ontological.NetDERKB import NetDERKB
from Ontological.NetDB import NetDB
from Ontological.NetDERChase import NetDERChase
from Ontological.NetDERQuery import NetDERQuery
from Ontological.NetDERTGD import NetDERTGD
from Ontological.NetDEREGD import NetDEREGD
from Ontological.Atom import Atom
from Ontological.GRE import GRE
from Ontological.Distinct import Distinct
from Ontological.Variable import Variable
from Ontological.Constant import Constant
from Ontological.PredictionsFakeNewsRob import PredictionsFakeNewsRob
from Ontological.Null import Null
from Ontological.NetCompTarget import NetCompTarget

def test1():
	nodes = [NetDiffNode('0'), NetDiffNode('1'), NetDiffNode('2'), NetDiffNode('3')]
	edges = [NetDiffEdge('0', '1'), NetDiffEdge('0', '2'), NetDiffEdge('0', '3'), NetDiffEdge('2', '3')]

	graph = NetDiffGraph('graph', nodes, edges)

	category_nlabels = []
	category_glabels = []
	category_kinds = ['A', 'B', 'C', 'D', 'E']
	for category in category_kinds:
		category_nlabels.append(NLocalLabel(category))
		category_glabels.append(GlobalLabel('trending(' + category + ')'))

	nodes[0].setLabels(category_nlabels)
	graph.setLabels(category_glabels)

	local_rules = []
	for nlabel in category_nlabels:
		local_rules.append(NetDiffLocalRule(nlabel, [], 1, [(nlabel, portion.closed(1, 1))], [], EnhancedTipping(0.5, portion.closed(1, 1))))

	global_rules = []
	index = 0
	for glabel in category_glabels:
		global_rules.append(NetDiffGlobalRule(glabel, category_nlabels[index], [], Average()))
		index += 1


	#atom4, atom5, atom6, atom7, atom8, atom9, atom10, atom11, atom12, ont_head1, ont_head2, ont_head3, ont_head5, ont_head9
	atom4 = Atom('earlyPoster', [Variable('UID'), Variable('FN')])
	atom5 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN1')])
	atom6 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN2')])
	atom7 = Distinct(Variable('FN1'), Variable('FN2'))
	atom8 = Atom('pre_hyp_fakenews', [Variable('FN')])
	atom9 = Atom('hyp_malicious', [Variable('UID1')])
	atom10 = Atom('hyp_malicious', [Variable('UID2')])
	atom11 = Atom('closer', [Variable('UID1'), Variable('UID2')])
	atom12 = Atom('pre_hyp_fakenews2', [Variable('FN')])



	ont_head1 = Atom('hyp_fakenews', [Variable('FN')])
	ont_head2 = Atom('hyp_is_resp', [Variable('UID'), Variable('FN')])
	ont_head3 = Atom('hyp_malicious', [Variable('UID')])
	ont_head5 = [Atom('hyp_botnet', [Variable('B')]), Atom('member', [Variable('UID1'), Variable('B')]), Atom('member', [Variable('UID2'), Variable('B')])]
	ont_head9 = Atom('hyp_my_resp', [Variable('FN'), Variable('UID')])

	global_conditions = []
	for glabel in category_glabels:
		global_conditions.append((glabel, portion.closed(0.1, 1)))

	tgds1 = []

	#news(FN, fake_level) ^ news_category(FN, Category) ^ (fake_level > \theta_1) -> hyp_fakeNews(FN) : trending(Category)
	#pre_hyp_fakenews(FN) ^ news_category(FN, Category) -> hyp_fakeNews(FN) : trending(Category)
	#"pre_hyp_fakenews(FN)" es un atomo preprocesado que encapsula la situacion en la que un determinado articulo es considerado fake news por una herramienta
	#externa asignando un valor de confianza que supera cierta umbral definido (\theta_1)
	tgd_counter = 0
	for gc in global_conditions:
		news_category_atom = Atom('news_category', [Variable('FN'), Constant(category_kinds[tgd_counter])])
		tgds1.append(NetDERTGD(rule_id = tgd_counter, ont_body = [atom8, news_category_atom], ont_head = [ont_head1], global_cond = [gc]))
		tgd_counter += 1

	#hyp_fakeNews(FN) ^ earlyPoster(UID, FN) ^ user(UID, N) -> hyp_is_resp(UID, FN)
	#hyp_fakeNews(FN) ^ earlyPoster(UID, FN) -> hyp_is_resp(UID, FN)

	tgd1 = NetDERTGD(rule_id = tgd_counter, ont_body = [ont_head1, atom4], ont_head = [ont_head2])
	tgd_counter += 1

	#hyp_is_resp(UID, FN1) ^ hyp_is_resp(UID, FN2) ^ (FN1 != FN2) -> hyp_malicious(UID)

	tgd2 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom5, atom6, atom7], ont_head = [ont_head3])
	tgd_counter += 1

	#hyp_malicious(UID1) ^ hyp_malicious(UID2) ^ closer(UID1, UID2) ^ (V > \theta_2) ^ (UID1 != UID2) -> \exists B hyp_botnet(B) ^ member(UID1, B) ^ member(UID2, B)
	tgd3 = NetDERTGD(rule_id = 'tgd' + str(tgd_counter), ont_body = [atom9, atom10, atom11], ont_head = ont_head5)
	tgd_counter += 1

	tgd4 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom12], ont_head = [ont_head1])
	tgd_counter += 1


	tgds1.append(tgd1)
	tgds1.append(tgd2)
	tgds1.append(tgd3)
	tgds1.append(tgd4)

	egds = []
	egd_counter = tgd_counter + 1
	#hyp_botnet(B1) ^ hyp_botnet(B2) ^ member(UID, B1) ^ member(UID, B2) ->  B1 = B2
	egd1 = NetDEREGD(rule_id = egd_counter, ont_body = [Atom('member', [Variable('UID'), Variable('B1')]), Atom('member', [Variable('UID'), Variable('B2')])], head = [Variable('B1'), Variable('B2')])

	egds.append(egd1)


	tmax = 2
	facts = []
	for n in nodes:
		facts.append(NetDiffFact(n, NLocalLabel('C'), portion.closed(1,1), 0, tmax))
	atoms1 = [Atom('pre_hyp_fakenews', [Constant('fn1')]), Atom('news_category', [Constant('fn1'), Constant('C')]), Atom('pre_hyp_fakenews', [Constant('fn2')]), Atom('news_category', [Constant('fn2'), Constant('C')]), Atom('earlyPoster', [Constant('1'), Constant('fn1')]), Atom('earlyPoster', [Constant('1'), Constant('fn2')]), Atom('earlyPoster', [Constant('2'), Constant('fn3')]), Atom('earlyPoster', [Constant('2'), Constant('fn4')]), Atom('closer', [Constant('1'), Constant('2')])]
	atoms2 = [Atom('pre_hyp_fakenews2', [Constant('fn3')]), Atom('pre_hyp_fakenews2', [Constant('fn4')])]
	#atoms es una lista de listas, cada posicion corresponde a un conjunto de atomos relativos 
	#a un tiempo determinado en orden creciente (de alguna manera definen un bloque de procesamiento)
	atoms = [atoms1, atoms2]
	kb = NetDERKB(ont_data = [], net_db = NetDB(graph, facts), netder_tgds = tgds1, netder_egds = egds, netdiff_lrules = local_rules, netdiff_grules = global_rules)

	chase = NetDERChase(kb, tmax)

	query1 = NetDERQuery(exist_var = [Variable('Botnet')], ont_cond = [Atom('hyp_fakenews', [Variable('HFN')]), Atom('hyp_is_resp', [Variable('HResp'), Variable('FN')]), Atom('hyp_malicious', [Variable('Mal')]), Atom('member', [Variable('UID1'), Variable('Botnet')])], time = (tmax, tmax))

	iteracion = 0
	for a in atoms:
		print('Current Time', iteracion)
		kb.add_ont_knowledge(a)
		answers = chase.answer_query(query1, 1)
		print("NetDER Query")
		print(query1)
		print('-----')
		for ans in answers:
			for key in ans.keys():
				print("Variable", key, "instanciada con valor", ans[key].getValue())
		iteracion += 1

body_symbol = 'news'
def get_random_news_atoms(cant):
	atoms = []
	for index in range(cant):
		value1 = ''.join(random.choices(string.ascii_letters, k=10))
		value2 = random.random()
		atom = Atom('news', [Constant(value1), Constant(value2)])
		atoms.append(atom)
	return atoms
#news(FN, fake_level)

def get_new_graph(num_nodes, num_edges):
	csv_graph_location = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/graph_structure/graph(n="+ str(num_nodes) +", e="+ str(num_edges) + ").csv"
	executable_location = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + '/executable/PaRMAT.exe'
	subprocess.call([executable_location, "-nEdges", str(num_edges), "-nVertices", str(num_nodes), "-noEdgeToSelf", "-noDuplicateEdges", "-output", csv_graph_location])
	graph = {}
	
	for n in range(num_nodes):
		if not (str(n) in graph.keys()):
			graph[str(n)] = {}
		graph[str(n)]["neigh"] = []

	with open(csv_graph_location, newline = '', encoding = 'utf-8') as csvfile:
		spamreader = csv.reader(csvfile, delimiter = '\t')
		for row in spamreader:
			graph[row[0]]["neigh"].append(row[1])
	return graph

def get_netDiffGraph(cant_nodes, cant_edges):
	graph = get_new_graph(cant_nodes, cant_edges)
	netDiffNodes = []
	netDiffEdges = []
	for node_id in graph.keys():
		netDiffNodes.append(NetDiffNode(node_id))
		for neigh_id in graph[node_id]['neigh']:
			netDiffEdges.append(NetDiffEdge(node_id, neigh_id))

	netDiffGraph = NetDiffGraph('graph', netDiffNodes, netDiffEdges)
	nlabels = [NLocalLabel('covid19 doesn\'t exist')]
	gLabels = [GlobalLabel('trending(covid19 doesn\'t exist)')]
	#configura las Node Local Labels que van a estar disponibles
	netDiffNodes[0].setLabels(nlabels)
	#configura las Global Labels que van a estar disponibles
	netDiffGraph.setLabels(gLabels)

	return netDiffGraph

def get_NetDiffFacts(elements, labels, tmax):
	result = []
	for elem in elements:
		if random.random() < 0.5:
			label = random.choice(labels)
			result.append(NetDiffFact(elem, label, portion.closed(1,1), 0, tmax))

	return result



def test2():
	tmax = 2


	inicio_graph = datetime.now()
	netDiffGraph = get_netDiffGraph(cant_nodes = 5000, cant_edges = 5000)
	fin_graph = datetime.now()
	print('tiempo creacion grafo', (fin_graph - inicio_graph))
	netDiffFacts = get_NetDiffFacts(elements= netDiffGraph.getNodes(), labels = netDiffGraph.getNodes()[0].getLabels(), tmax = tmax)

	#selecciono la unica node local label disponible
	nlabel = netDiffGraph.getNodes()[0].getLabels()[0]
	local_rules = [NetDiffLocalRule(nlabel, [], 1, [(nlabel, portion.closed(1, 1))], [], EnhancedTipping(0.5, portion.closed(1, 1)))]

	#selecciono la unica global label disponible
	glabel = netDiffGraph.getLabels()[0]
	global_rules = [NetDiffGlobalRule(glabel, nlabel, [], Average())]
	#---------------------------------------------------------------------------

	#"atoms" lo voy a utilizar luego para crear la BD ontologica
	cantidad_atomos = 1000
	atoms = get_random_news_atoms(cantidad_atomos)
	atom1 = Atom('news', [Variable('Content'), Variable('FN_level')])
	atom2 = GRE(Variable('FN_level'), Constant(0.1))
	ont_head1 = Atom('hyp_fakenews', [Variable('Content')])
	tgd_counter = 0
	#news(Content, FN_level) ^ FN_level > 0.1 -> hyp_fakenews(Content) : (trending(trending(covid19 doesn\'t exist), [0.3,1]))
	tgd1 = NetDERTGD(rule_id = tgd_counter, ont_body = [atom1, atom2], ont_head = [ont_head1], global_cond = [(glabel, portion.closed(0.3,1))])


	kb = NetDERKB(ont_data = atoms, net_db = NetDB(netDiffGraph, netDiffFacts), netder_tgds = [tgd1], netdiff_lrules = local_rules, netdiff_grules = global_rules)

	chase = NetDERChase(kb, tmax)
	#query1(HFN) = hyp_fakenews(HFN):[tmax, tmax], tener en cuenta que en este caso el tiempo no tiene importancia
	query1 = NetDERQuery(ont_cond = [Atom('hyp_fakenews', [Variable('HFN')])], time = (tmax, tmax))
	answers = chase.answer_query(query1, 1)
	print("NetDER Query")
	print(query1)
	print('-----')
	for ans in answers:
		for key in ans.keys():
			print("Variable", key, "instanciada con valor", ans[key].getValue())

inicio = datetime.now()

#test1()
test2()



print() 

fin = datetime.now()

print('tiempo total:', (fin - inicio))