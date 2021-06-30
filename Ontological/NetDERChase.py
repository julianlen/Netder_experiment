import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import copy
import bisect
from datetime import datetime
from Ontological.Variable import Variable
from Ontological.Null import Null
from Ontological.RDBHomomorphism import RDBHomomorphism
from Diffusion_Process.NetDiffProgram import NetDiffProgram
from Diffusion_Process.NetDiffInterpretation import NetDiffInterpretation
from Diffusion_Process.NetDiffFact import NetDiffFact

class NetDERChase:

	def __init__(self, kb, tmax = 1, config_db = 'config_db.json'):
		self._kb = kb
		self._tmax = tmax
		self._config_db = config_db
		self._net_diff_interpretation = NetDiffInterpretation(self._kb.get_net_diff_graph(), self._tmax)
		

	#el parametro "id_atoms" se utiliza para diferenciar dos conjuntos de atomos sintacticamente iguales pero que corresponden a reglas diferentes
	#esto evita que una vez que un mapeo sea utilizado para el cuerpo de una regla luego ya no pueda ser utilizado para otra con un cuerpo sintacticamente igual
	def _get_atoms_mapping(self, atoms, id_atoms = 0, historical_included = False):
		h = RDBHomomorphism(self._kb)
		#se busca mapaer los atomos "atoms" en la base de datos a traves de un homomorfismo h
		result = h.get_atoms_mapping(atoms, id_atoms, historical_included)
		return result



	#obtiene todos los mapeos posibles del cuerpo de la regla "rule" en el tiempo "time", considerando la base de datos de la KB self._kb
	def get_body_mapping(self, rule, time):
		net_db = []
		cloned_net_body = copy.deepcopy(rule.get_net_body())
		for nct in cloned_net_body:
			net_db.append(nct.getComponent())
		#net_db almacena los atomos involucrados en el cuerpo de la regla (rule) y que estan referidos a nodos o arcos de la red


		#se busca mapear los atomos del cuerpo ontologico y los atomos relativos de nodos y arcos, en la base de datos
		body_mapping = self._get_atoms_mapping(rule.get_ont_body() + net_db, rule.get_id())


		aux_body_mapping = {}
		if len(body_mapping) > 0:

			for key in body_mapping.keys():
				
				if (len(time) > 0):
					#almacena los componentes del grafo (nodos o arcos) mapeados de acuerdo "body_mapping[key]"
					net_db = []
					cloned_net_body = copy.deepcopy(rule.get_net_body())
					for nct in cloned_net_body:
						net_db.append(nct.getComponent())
					for atom in net_db:
						atom.map(body_mapping[key])
					#se verifica si hay condiciones locales de la red por verificar
					if len(cloned_net_body) > 0:
						for nct in cloned_net_body:
							comp = self._kb.get_comp_from_atom(nct.getComponent())
							for t in range(time[0], time[1] + 1):
								#se verifican las condiciones locales de la regla para cada tiempo
								if self._net_diff_interpretation.isSatisfied(t, comp, (nct.getLabel(), nct.getBound())):
									#se verifican las condiciones globales de la regla para cada tiempo
									if self._net_diff_interpretation.areSatisfied(t, self._kb.get_net_diff_graph(), rule.get_global_cond()):
										aux_body_mapping[key] = body_mapping[key]

					else:
						for t in range(time[0], time[1] + 1):
							#se verifican las condiciones globales de la regla para cada tiempo
							if self._net_diff_interpretation.areSatisfied(t, self._kb.get_net_diff_graph(), rule.get_global_cond()):
								aux_body_mapping[key] = body_mapping[key]
					
				else:
					#si no se especifica tiempo no se verifican condiciones de la red
					aux_body_mapping = body_mapping

		body_mapping = aux_body_mapping

		return body_mapping


	def applyStepTGDChase(self, tgd, time):
		result = []
		applicable = True

		body_mapping = self.get_body_mapping(tgd, time)
		#almacena todas la posibles formas (una por cada mapeo posible) de nuevo conocimiento ontologico obtenido de aplicar la tgd
		ont_head_result = []
		#almacena todas la posibles formas (una por cada mapeo posible) de nuevo conocimiento de red obtenido de aplicar la tgd
		net_head_result = []
		#se verifica si hay algun mapeo
		if len(body_mapping) > 0:
			
			for key_pos in body_mapping.keys():
				#se aplica cada mapeo disponible en la cabeza de la tgd
				#se obtiene conocimiento ontologico y de red
				#"net_head_comp" almacena una copia de la parte de red de la cabeza de la tgd relativa a los componentes del grafo (nodos o arcos)
				net_head_comp = []
				cloned_net_head = copy.deepcopy(tgd.get_net_head())
				for nct in cloned_net_head:
					net_head_comp.append(nct.getComponent())
				#"cloned_ont_head" almacena una copia de la parte ontologica de la cabeza de la tgd
				cloned_ont_head = copy.deepcopy(tgd.get_ont_head())
				
				#se aplica cada mapeo a cada atomo de la parte ontologica de la tgd
				for atom in cloned_ont_head:
					atom.map(body_mapping[key_pos])
				#se aplica cada mapeo a cada componente del grafo de la parte de red de la cabeza de la tgd
				for comp in net_head_comp:
					comp.map(body_mapping[key_pos])
				ont_head_result.append(cloned_ont_head)
				net_head_result.append(cloned_net_head)
		#se almacena el nuevo conocimiento obtenido por la tgd
		#el primer elemento de la lista contiene todas la posibles formas de nuevo conocimiento ontologico obtenido de aplicar la tgd
		#el segunfo elemento de la lista contiene todas la posibles formas de nuevo conocimiento de red obtenido de aplicar la tgd
		aux_result = [set(), set()]
		index = 0
		#cada elemento (ontologico o de red) de la cabeza de la tgd se encuentra mapeado, excepto las variables existenciales
		#para cada variable existencial se crea un null, a traves de un mapeo que se aplica a toda la cabeza de la tgd
		for possibility in ont_head_result:
			for atom in copy.deepcopy(possibility):
				for variable in atom.get_variables():
					#Si aun queda alguna variable significa que esta existencialmente cuantificada
					null = self._kb.create_null()
					for other_atom in possibility:
						#se aplica el mapeo a cada atomo, para asignar el Null creado anteriormente
						other_atom.map({variable.getId(): null})
						self._kb.save_null_info(other_atom, null)
					for nct in net_head_result[index]:
						#se aplica el mapeo a cada componente de red de la cabeza, para asignar el Null creado anteriormente
						nct.getComponent().map({variable.getId(): null})
						self._kb.save_null_info(nct.getComponent(), null)
			#los atomos de la parte ontologica de la cabeza de la tgd totalmente mapeados se almacenan en la primer posicion de "aux_result"
			aux_result[0] = aux_result[0].union(set(possibility))

		#posiblemente aun existan variables existenciales en la parte de red de la cabeza de la tgd
		#se repite un proceso similar al de la parte ontologica, para instanciar variables existenciales restantes en la parte de red

		for possibility in net_head_result:
			net_diff_knwl = set()
			for nct in possibility:
				for variable in nct.getComponent().get_variables():
					null = self._kb.create_null()
					for nct in possibility:
						nct.getComponent().map({variable.getId(): null})
						self._kb.save_null_info(nct.getComponent(), null)
				net_diff_knwl.add(NetDiffFact(nct.getComponent(), nct.getLabel(), nct.getBound(), time[0], time[1]))
				net_diff_knwl.add(nct.getComponent())
			#el nuevo conocimiento totalmente mapeado se almacena en la segunda posicion de "aux_result"
			aux_result[1] = aux_result[1].union(net_diff_knwl)



		return aux_result

	def applyStepEGDChase(self, egd, time):
		success = True
		#se busca si existe algun mapeo del cuerpo
		body_mapping = self.get_body_mapping(egd, time)

		#se verifica si hay algun mapeo
		if len(body_mapping) > 0:
			#a continuacion, se verifica que en cada mapeo se cumpla la restriccion de la cabeza (por ejemplo, X = Y)
			for key_pos in body_mapping.keys():
				new_mapping = {}
				#para cada mapeo la verificacion se realiza en tres pasos:
				#paso (1): se realizan dos copias (llamemosla (a) y (b)) del cuerpo de la regla (divididas en parte ontologica y de red)
				#paso (2.1): se aplica el mapeo tomando como clave la primer parte de la cabeza y como valor la segunda parte de la cabeza
				#paso (2.2): en la parte ontologica y en la parte de red de la copia (a). Por ejemplo si la cabeza es "X=Y", se aplica el mapeo "{'X': Y}"
				#paso (3): se aplica el mapeo body_mapping[key_pos] en la parte ontologica y de red de la copia (a)
				#paso (4.1): se aplica el mapeo tomando como clave la segunda parte de la cabeza y como valor la primer parte de la cabeza
				#paso (4.2): en la parte ontologica y en la parte de red de la copia (b). Por ejemplo si la cabeza es "X=Y", se aplica el mapeo "{'Y': X}"
				#paso (5): se aplica el mapeo body_mapping[key_pos] en la parte ontologica y de red de la copia (b)
				#paso (6.1): se verifica que las copias (a) y (b), mapeadas segun los pasos anteriores, cumplan que cada par de terminos "equivalentes"
				#paso (6.2): alguno de los dos terminos de una de las copias es un Null (en ese caso se construye un mapeo para actualizar la BD)
				#paso (6.3): ambos terminos de las copias son constantes e iguales.
				#paso (6.4): caso contrario la EGD no se satisface.
				#paso (6.4): Por ejemplo, si se tiene la copia (a) = p(z_1, z_2, a) y la copia (b) p(z_3, b, a) cumplen porque
				#paso (6.4): para el par "z_1" y "z_2" ambos son nulls, para el par "z_2" y "b" el primero es null, y para el par "a" y "a" son iguales
				head = copy.deepcopy(egd.get_head())
				#paso (1)
				cloned_ont_body1 = copy.deepcopy(egd.get_ont_body())
				cloned_net_body1 = copy.deepcopy(egd.get_net_body())
				cloned_ont_body2 = copy.deepcopy(egd.get_ont_body())
				cloned_net_body2 = copy.deepcopy(egd.get_net_body())
				
				for atom in cloned_ont_body1:
					#paso (2) en la parte ontologica
					atom.map({head[0].getId(): head[1]})
					#paso (3) en la parte ontologica
					atom.map(body_mapping[key_pos])

				for nct in cloned_net_body1:
					#paso (2) en la parte de red
					nct.getComponent().map({head[0].getId(): head[1]})
					#paso (3) en la parte de red
					nct.getComponent().map(body_mapping[key_pos])

				head = copy.deepcopy(egd.get_head())

				
				for atom in cloned_ont_body2:
					#paso (4) en la parte ontologica
					atom.map({head[1].getId(): head[0]})
					#paso (5) en la parte ontologica
					atom.map(body_mapping[key_pos])
				
				for nct in cloned_net_body2:
					#paso (4) en la parte de red
					nct.getComponent().map({head[1].getId(): head[0]})
					#paso (5) en la parte de red
					nct.getComponent().map(body_mapping[key_pos])
					

				for index in range(0, len(cloned_ont_body1)):
					term_i = 0
					for term in cloned_ont_body1[index].get_terms():
						#paso (6.2) para la parte ontologica
						other_term = cloned_ont_body2[index].get_terms()[term_i]
						if term.getValue() != other_term.getValue():
							if term.can_be_instanced():
								if other_term.can_be_instanced() and term.getValue() < other_term.getValue():
									#reemplazar "other_term" por "term" ya que "term" esta lexicograficamente antes "other_term"
									new_mapping[other_term.getValue()] = term
								else:
									#reemplazar "term" por "other_term" ya que "other_term" esta lexicograficamente antes "term"
									new_mapping[term.getValue()] = other_term
								
							#paso (6.2) para la parte ontologica
							elif other_term.can_be_instanced():
								#se construye un mapeo para actualizar la BD
								new_mapping[other_term.getValue()] = term
							#paso (6.3) para la parte ontologica
							#elif (not term.getValue() == cloned_ont_body2[index].get_terms()[term_i].getValue()):
							else:
								#los terminos no son iguales y ninguno de los dos es null (no puede ser instanciado)
								#paso (6.4) para la parte ontologica
								success = False
								break
								break

						term_i = term_i + 1
				
				
				if success:
					for index in range(0, len(cloned_net_body1)):
						term_i = 0
						for term in cloned_net_body1[index].getComponent().get_terms():
							#paso (6.2) para la parte de red
							other_term = cloned_net_body2[index].getComponent().get_terms()[term_i]
							if term.getId() != other_term.getId():
								if term.can_be_instanced():
									if other_term.can_be_instanced() and term.getValue() < other_term.getValue():
										#reemplazar "other_term" por "term" ya que "term" esta lexicograficamente antes "other_term"
										new_mapping[other_term.getValue()] = term
									else:
										#reemplazar "term" por "other_term" ya que "other_term" esta lexicograficamente antes "term"
										new_mapping[term.getValue()] = other_term
								
								#paso (6.2) para la parte de red
								elif other_term.can_be_instanced():
									#se construye un mapeo para actualizar la BD
									new_mapping[other_term.getValue()] = term
								#paso (6.3) para la parte de red
								#elif (not term.getValue() == cloned_net_body2[index].getComponent().get_terms()[term_i].getValue()):
								else:
									#paso (6.4) para la parte de red
									success = False
									break
									break
							term_i = term_i + 1
				if (success):
					#si la EGD se safisface para el mapeo body_mapping[key_pos], se actualizan los nulls (en caso de ser necesario)
					self._kb.update_nulls(new_mapping)
				else:
					break

		return success

	def answer_query(self, query, int_bound):
		result = []
		seguir = True
		counter = 0
		#se inicializa la interpretacion para la difusion, donde cada etiqueta tiene incertidumbre maxima, es decir, intervalo [0,1]
		#en ese caso, las unicas consultas de red que pueden satisfacerse son las que tengan como intervalo [0,1]
		#esas consultas no tienen mucho sentido porque la respuesta obtenida no te proporciona informacion en si
		self._net_diff_interpretation = NetDiffInterpretation(self._kb.get_net_diff_graph(), self._tmax)
		#se realizan iteraciones hasta una determinada cota entera (politica) o una condicion interna se cumpla
		while(counter <= int_bound and seguir):
			mapping = {}
			while(seguir):
				#"new_knowledge" almacena el nuevo conocimiento obtenido de aplicar TGDs
				#el primer elemento contiene conocimiento ontologico y el segundo conocimiento de red
				new_knowledge = [set(), set()]
				index = 0
				#se realiza un paso de aplicacion por cada TGD disponible
				for tgd in self._kb.get_netder_tgds():
					inicio = datetime.now()
					TGD_result = self.applyStepTGDChase(tgd, query.get_time())
					fin = datetime.now()
					index += 1
					new_knowledge[0] = new_knowledge[0].union(TGD_result[0])
					new_knowledge[1] = new_knowledge[1].union(TGD_result[1])
				
				#se incorpora el nuevo conocimiento ontologico obtenido
				#la operacion tiene exito si al menos uno de los atomos agregados es "nuevo"
				success = self._kb.add_ont_data(new_knowledge[0])
				#se incorpora el nuevo conocimiento de red obtenido
				#---------------
				#TENGO QUE ARREGLAR ESTO
				#self._kb.add_net_knowledge(new_knowledge[1], query.get_time())
				#-----------------
				self._kb.add_ont_data(new_knowledge[1])
				#se verifican si cada una de las EGDs se satisfacen, si alguna falla, se termina el proceso
				for egd in self._kb.get_netder_egds():
					#seguir = self.applyStepEGDChase(egd, query.get_time())
					success_egds = self.applyStepEGDChase(egd, query.get_time())
					if not success_egds:
						raise Exception('Una de las EGDs ha sido violada')

				#se reinicia este atributo porque los mapeos deben calcularse de nuevo (podria haber una forma de no reiniciar pero no creo que sea sencilla)
				self._body_mapping_his = []
				if seguir:
					qa_success = True
					mapping = {}
					#se buscan todos los mapeos para la consulta
					for q in query.get_disjoint_queries():
						#candidates = self._get_candidate_atoms(q)
						q_mapping = self._get_atoms_mapping(q.get_ont_body(), historical_included = True)
						
						mapping.update(q_mapping)
						
						if not (len(q_mapping) > 0):
							#si no se encuentra algun mapeo
							qa_success = False
					if (qa_success) or ((not success) and (len(new_knowledge[1]) == 0)):
						#si se se tuvo exito en la consulta o no se agrego nuevo conocimiento
						seguir = False
				else:
					mapping = {}

			#me parece las dos lineas siguientes no sirven para nada
			#if not qa_success and len(mapping) > 0:
			#	qa_success = True

			#se lleva adelante el proceso de difusion
			net_diff_program = NetDiffProgram(self._kb.get_net_diff_graph(), self._tmax, self._kb.get_net_diff_facts(), self._kb.get_net_diff_lrules(), self._kb.get_net_diff_grules())
			self._net_diff_interpretation = net_diff_program.diffusion()
			result = None
			seguir = True
			counter = counter + 1
			print('final net_diff_interpretation')
			print(str(self._net_diff_interpretation))
		#cuando las iteraciones terminan se deben construir las respuestas, que son mapeos de las variables no cuantificadas a valores de la base de datos
		result = []
		for key_pos in mapping.keys():
			aux_result_mapping = {}
			for key in mapping[key_pos].keys():
				if not (Variable(key) in query.get_exist_var()):
					aux_result_mapping[key] = mapping[key_pos][key]
			if len(aux_result_mapping) > 0:
				result.append(aux_result_mapping)

		return result

