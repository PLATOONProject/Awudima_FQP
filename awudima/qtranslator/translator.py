__author__ = "Kemele M. Endris"


from typing import List, Dict
import abc
import hashlib

from awudima.pysparql import Service


class GenericTranslator:
    def __init__(self, service_query: Service, prefixes: Dict = None):
        """
        :param service_query:
        :param prefixes:
        """
        self.service = service_query
        self.prefixes = prefixes
        if self.prefixes is None:
            self.prefixes = {}

    @staticmethod
    def split_triple_patterns(triple_patterns):
        """
        Split list of triple patterns into to lists: one list for variable predicate triple patterns,
        the second one for constant predicate triple patterns.

        :param triple_patterns: List of SPARQL triple patterns
        :return: tuple of triple patterns with variable predicate, triple patterns with constant predicate .
        """
        constant_tps = []
        constant_vars = []

        variable_tps = []
        variable_vars = []
        for tp in triple_patterns:
            if tp.predicate.constant:
                constant_tps.append(tp)
                constant_vars.extend(tp.getVars())
            else:
                variable_tps.append(tp)
                variable_vars.extend(tp.getVars())

        return variable_tps, variable_vars, constant_tps, constant_vars

    @abc.abstractmethod
    def translate(self):
        """
        translate the whole BGP to underlying data source query language (subject-[predicate, object] maps)
        :return: SQL query

        """
        return None

    @abc.abstractmethod
    def translate_star(self, star):
        pass

    def translate_triple_patterns(self, triple_map, triple_patterns: List, variables: List[str]):
        """
        Give a list of triple patterns, translate it to underlying data source query language

        :param triple_map:
        :param triple_patterns:
        :param prefixes: dict of prefixed as {rdf: http://...}
        :param variables:
        :return:
        """
        # variable_tps, variable_vars, constant_tps, constant_vars = GenericTranslator.split_triple_patterns(triple_patterns)
        # if len(variable_tps) == 0:
        return self.translate_const_predicates(triple_map, triple_patterns, variables)
        # else:
        #     return self.translate_var_predicate(triple_map, variable_tps, variable_vars, constant_tps, constant_vars, prefixes, variables)

    @abc.abstractmethod
    def translate_const_predicates(self, triple_map, triple_patterns: List, variables: List[str]):
        """
        Given a set of triple patterns with constant predicates, find matching predicates and translate them
         to underlying data source query language.

        :param triple_map:
        :param triple_patterns: List of SPARQL triple patterns
        :param prefixes:  dict of prefixed as {rdf: http://...}
        :return:
        """

        return None

    @abc.abstractmethod
    def translate_var_predicate(self, triple_map,
                                variable_tps: List, variable_vars: List,
                                constant_tps: List, constant_vars: List,
                                variables: List[str]):
        """
        Given a set of triple patterns that contain at least one variable predicate, translate all predicates
        to SQL and matching constant predicates.
        This differs from self.translate() as this one is column:value selection while the former is per row selection.

        :param triple_map:
        :param variable_tps: List of SPARQL triple patterns with variable predicate term
        :param constant_tps: List of SPARQL triple patterns with constant predicate term
        :param prefixes:  dict of prefixed as {rdf: http://...}
        :return:
        """

        return None

    @abc.abstractmethod
    def translate_subject(self, triple_map, rdf_term):
        return None


