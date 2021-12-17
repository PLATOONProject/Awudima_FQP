__author__ = "Kemele M. Endris"

from typing import List, Dict
import abc
from awudima.pysparql import Service, Filter
from awudima.mongoql.ld_flat.Star2MongoLDF import Star2MongoLDF
from awudima.pyrdfmt import Federation


class MongoTranslator:
    def __init__(self, service_query: Service, prefixes: Dict = None):
        """
        :param service_query:
        :param prefixes:
        """
        self.service = service_query
        self.prefixes = prefixes
        if self.prefixes is None:
            self.prefixes = {}

    def get_collection_names(self, rdfmt, mtpreds):
        rml_source = rdfmt.predicate_RML_sources
        sources = set()
        first = True

        for pred in mtpreds:
            predicate_uri = pred.predId # SPARQL.getUri(pred, self.prefixes)[1:-1]
            psources = [p.ds_desc['collection_name'] for p in rml_source[predicate_uri]]
            if first:
                for s in psources:
                    # print(type(s), s.name)
                    sources.add(s)
                first = False
            sources = sources.union(psources)
        return list(sources)

    @staticmethod
    def split_triple_patterns(triple_patterns):
        """
        Split list of triple patterns into to lists:
        - one list for variable predicate triple patterns,
        - the second one for constant predicate triple patterns.

        :param triple_patterns: List of SPARQL triple patterns
        :return: tuple of triple patterns with variable predicate,
                  triple patterns with constant predicate .
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
        translate the whole BGP to mongodb query language (subject-[predicate, object])
        :return: MONGO query

        """
        return None

    @abc.abstractmethod
    def translate_star(self, star):
        pass

    def translate_triple_patterns(self,
                                  triple_map,
                                  triple_patterns: List,
                                  variables: List[str]):
        """
        Give a list of triple patterns, translate it to mongodb query language

        :param triple_map:
        :param triple_patterns:
        :param variables:
        :return:
        """
        # variable_tps, variable_vars, constant_tps,
        # constant_vars = MongoTranslator.split_triple_patterns(triple_patterns)
        # if len(variable_tps) == 0:
        return self.translate_const_predicates(triple_map, triple_patterns, variables)
        # else:
        #     return self.translate_var_predicate(triple_map,
        #     variable_tps, variable_vars, constant_tps, constant_vars, prefixes, variables)

    @abc.abstractmethod
    def translate_const_predicates(self, triple_map, triple_patterns: List, variables: List[str]):
        """
        Given a set of triple patterns with constant predicates,
         find matching predicates and translate them
         to underlying data source query language.

        :param triple_map: RML mapping for the data collections
        :param triple_patterns: List of SPARQL triple patterns
        :param variables: list of variables in the triple patterns list
        :return:
        """

        return None

    @abc.abstractmethod
    def translate_var_predicate(self, triple_map,
                                variable_tps: List, variable_vars: List,
                                constant_tps: List, constant_vars: List,
                                variables: List[str]):
        """
        Given a set of triple patterns that contain at least one variable predicate,
        translate all predicates to MongoDB query and matching constant predicates.
        This differs from self.translate() as this one is column:value selection while the former is per row selection.

        :param triple_map:
        :param variable_tps: List of SPARQL triple patterns with variable predicate term
        :param constant_tps: List of SPARQL triple patterns with constant predicate term
        :param variables: list of variables in the triple patterns list
        :return:
        """

        return None


class MongoLDFlatTranslator(MongoTranslator):
    def __init__(self, service: Service, prefixes, config: Federation):
        super(MongoLDFlatTranslator, self).__init__(service, prefixes)
        self.config = config
        self.datasource = service.datasource
        self.database_name = None
        dbkey = '<http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN>'
        if self.datasource is not None and dbkey in self.datasource.params:
            self.database_name = self.datasource.params[dbkey]
        self.stars = service.stars

    def translate(self):
        star_queries = {}
        star_variables = {}
        sparql_result_templates = {}
        for star_var, star in self.stars.items():
            star_query, outer_collection, sparql_result_template = self.translate_star(star)
            sparql_result_templates.update(sparql_result_template)
            star_queries[star_var] = (star_query, outer_collection)
            star_vars = ['?' + v for v in sparql_result_template.keys()]
            star_variables.setdefault(star_var, []).extend(star_vars)
            if star_var in star_variables[star_var]:
                star_variables[star_var].remove(star_var)

        variables = list(set([v.lstrip("?$") for v in self.service.getVars()]))
        constants = list(set([v for v in self.service.getConsts()]))

        # if multiple ssqs are found, then pushdown joins
        if len(star_queries) > 1:
            # sql_query = self._join_star_queries(star_queries, star_variables)
            pass
        elif len(star_queries) == 1:
            mongo_query, outer_collection = list(star_queries.values())[0]
            return {"pipeline": mongo_query, 'collection': outer_collection, 'db': self.database_name}, variables, constants, sparql_result_templates
        else:
            return None, None, None, None
            # raise ValueError("No translation found for the given sub-query. ", self.service)

        return {"pipeline": star_queries, 'collection': outer_collection, 'db': self.database_name}, variables, constants, sparql_result_templates

    def translate_star(self, star):
        sparql_result_templates = {}
        collection_queries = {}
        # source = self.config.datasources_obj[self.datasource.dsId]
        star_rdfmts = star['datasources'][self.datasource.dsId]
        star_query = None
        outer_collection = []
        for mtID, mtpredicates in star_rdfmts.items():
            rdfmt = self.config.rdfmts_obj[mtID]
            # collections = self.get_collection_names(rdfmt, mtpredicates)
            # for collection_name in collections:
            star_query, collections, sparql_result_template = self.translate_const_predicates(rdfmt, star['triples'], star['variables'])
            outer_collection.extend(collections)
#            collection_queries[outer_collection] = star_query
#            sparql_result_templates.update(sparql_result_template)

        return star_query, outer_collection, sparql_result_template

        # if len(collection_queries) == 1:
        #     outer_collection = list(collection_queries.keys())[0]
        #     return collection_queries[outer_collection], outer_collection, sparql_result_templates
        # elif len(collection_queries) > 1:
        #     colls = list(collection_queries.keys())
        #     outer_collection = colls[0]
        #     star_query = collection_queries[outer_collection]
        #     for coll in colls[1:]:
        #
        #         star_query.append(
        #             {"$unionWith": {"coll": coll, "pipeline": collection_queries[coll]}}
        #         )
        #     return star_query, outer_collection, sparql_result_templates
        # return star_query, outer_collection, sparql_result_templates

    def translate_const_predicates(self, rdfmt, triple_patterns: List, variables: List[str]):

        query_filters = [f for f in self.service.filters if isinstance(f, Filter)]
        star_query = Star2MongoLDF(rdfmt, triple_patterns, query_filters, variables, self.prefixes, self.datasource)

        return star_query.mongo_ql

    def translate_var_predicate(self, triple_map, variable_tps: List, variable_vars: List, constant_tps: List,
                                constant_vars: List, variables: List[str]):
        pass
