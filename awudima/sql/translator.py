__author__ = "Kemele M. Endris"

from awudima.sql.lang.model import SQLSelectQuery, SQLUnions, SQLJoins, SQLSelectExpression, SQLCondition, SQLColumn
from awudima.sql.rml2sql.triple_map import TripleMap2SQL
from awudima.sql.rml2sql.logical_source import LogicalSource2SQL
from awudima.sql.rml2sql.term_map import TermMap2SQL
from awudima.qtranslator import GenericTranslator
from typing import Dict, List
from awudima.pysparql import Service, Filter, SPARQL
from awudima.pyrdfmt import Federation


class SQLTranslator(GenericTranslator):
    _global_table_counter = 0

    @staticmethod
    def get_global_table_counter(prefix='table_name_'):
        SQLTranslator._global_table_counter += 1
        return prefix + str(SQLTranslator._global_table_counter)

    def __init__(self, service: Service, prefixes, config: Federation, dbms='MySQL'):
        super(SQLTranslator, self).__init__(service, prefixes)
        self.dbms = dbms
        self.config = config
        self.datasource = service.datasource
        self.stars = service.stars
        self.rml = self.datasource.mappings

    def translate(self):
        """
        translate the whole triple map to SQL query (subject-[predicate, object] maps)
        ```SELECT *
            FROM logical_source.table/query
            WHERE subject_map.columns is not Null or empty
           ```
        Note: in this case predicate object maps are optional. Only the subject has to be non null
        :return: SQL query - SQLJoins(), SQLUnions(), SQLQuery() or TripleMap2SQL() object

        """
        # A star-shaped subquery have exactly one translation. In some cases the service query received for translation
        # might have more than one star-shaped sub-queries; pushed down for efficiency of joins by the dbms.
        star_queries = {}
        # A set of variables of a star-shaped sub-query that will be used for joining between
        # multiple star-shaped sub-queries
        # this should not be confused with join conditions between multiple tables in case of ReferenceObjectMaps
        star_variables = {}
        sparql_result_templates = {}
        # perform query translation for each star shaped sub-query
        for star_var, star in self.stars.items():
            star_query, sparql_result_template = self.translate_star(star)
            sparql_result_templates.update(sparql_result_template)
            star_queries[star_var] = star_query
            star_vars = ['?' + v for v in sparql_result_template.keys()]
            star_variables.setdefault(star_var, []).extend(star_vars)
            # Since the join will be on the subject variable, we remove it from star variables
            # i.e., predicate and object variables, mostly joins are from subject to object part of stars
            if star_var in star_variables[star_var]:
                star_variables[star_var].remove(star_var)

        # if multiple ssqs are found, then pushdown joins
        if len(star_queries) > 1:
            sql_query = self._join_star_queries(star_queries, star_variables)
        elif len(star_queries) == 1:
            sql_query = list(star_queries.values())[0]
        else:
            raise ValueError("No translation found for the given sub-query. ", self.service)

        # the star_query property of TripleMap2SQL represent SQLQuery() object
        # if sql_query is not TripleMap2SQL, then it is either SQLUnion, SQLJoin or SQLQuery
        if isinstance(sql_query, TripleMap2SQL):
            sql_query, sparql_result_templates = sql_query.star_query

        variables = list(set([v.lstrip("?$") for v in self.service.getVars()]))
        constants = list(set([v for v in self.service.getConsts()]))

        # print("**************BGP_Query********************")
        # print(sql_query)
        # print('----------Template------')
        # from pprint import pprint
        # pprint(sparql_result_templates)

        return sql_query, variables, constants, sparql_result_templates

    def _join_star_queries(self, star_queries: Dict, star_variables: Dict):
        """
        Formulate SQL Join query between star-shaped-sub-queries.
        This pushes down joins to the database management system.

        :param star_queries: dictionary of query translations, one entry per star-shaped-sub-query
                                {star_var: TripleMap2SQL or SQLUnions}
        :param star_variables: dictionary of variables, one per sssq {star_var: [var1, var2, ...]
        :return: SQLJoins() object that represent SQL JOIN statement
        """
        queries = [starv for starv, q in star_queries.items()]
        star_vars = [v for v in self.stars]
        joined_stars = {}
        latest_join_var = None
        while len(queries) > 0:
            if len([False for v in star_vars if v not in joined_stars]) == 0:
                break
            star_var = queries.pop()
            obj_vars = star_variables[star_var]
            query = star_queries[star_var]
            for var in obj_vars:
                if var in star_queries:
                    if var in joined_stars:
                        if joined_stars[var] == star_var:
                            continue
                        join_query = star_queries[joined_stars[var]]
                        new_var = joined_stars[var]
                        joined_stars[new_var] = star_var + '_' + new_var
                        joined_stars[star_var] = star_var + '_' + new_var
                        if new_var in queries:
                            queries.remove(new_var)
                        queries.append(star_var + '_' + new_var)
                    else:
                        join_query = star_queries[var]
                        new_var = var
                        joined_stars[var] = star_var + '_' + var
                        joined_stars[star_var] = star_var + '_' + var
                        if var in queries:
                            queries.remove(var)
                        queries.append(star_var + '_' + var)

                    latest_join_var = star_var + '_' + new_var
                    base_query_alias = SQLTranslator.get_global_table_counter()
                    join_query_alias = SQLTranslator.get_global_table_counter()
                    join_conditions = SQLCondition(SQLColumn('`' + var[1:] + '`', base_query_alias), '=',
                                                   SQLColumn('`' + var[1:] + '`', join_query_alias))

                    projections = [SQLSelectExpression(SQLColumn('`' + v[1:] + '`'))
                                   for v in star_variables[new_var] if v not in obj_vars]
                    projections.extend(
                        [SQLSelectExpression(SQLColumn('`' + variable[1:] + '`', base_query_alias), variable[1:])
                         for variable in obj_vars])
                    if star_var not in list(joined_stars.values()):
                        projections.append(
                            SQLSelectExpression(SQLColumn('`' + star_var[1:] + '`', base_query_alias), star_var[1:]))

                    join_query = SQLJoins(query, {join_query_alias: join_query}, join_conditions, base_query_alias,
                                          projections)
                    star_queries[star_var + '_' + new_var] = join_query
                    star_variables[star_var + '_' + new_var] = list(
                        set(star_variables[new_var] + star_variables[star_var]))
                    if star_var not in list(joined_stars.values()):
                        star_variables[star_var + '_' + new_var].append(star_var)
                    if var not in list(joined_stars.values()):
                        star_variables[star_var + '_' + new_var].append(var)

        if latest_join_var in star_queries:
            sql_query = star_queries[latest_join_var]  # self.join_stars(star_queries, join_variables)
        else:
            raise ValueError("No translation found for the given sub-query. ", self.service)
        return sql_query

    def translate_star(self, star):
        """
        Translate a Star-shaped sub-query (SSQ) to SQL statement based on the triple maps defined in the
        selected data source for this SSQ

        :param star: Star-shaped Sub-query to be translated to SQL
        :return: SQLUnion or SQLJoin or TripleMap2SQL object
        """
        triple_map_queries = []
        sparql_result_templates = {}

        partial_matching_triple_map_queries = []
        # iterate over star shaped sub-query's source selection with matched RDF Molecule templates
        source = self.config.datasources_obj[self.datasource.dsId]
        star_rdfmts = star['datasources'][self.datasource.dsId]
        # Complete source description from federation metadata that includes the rml mappings
        # source = self.config.datasources_obj[dsId]
        processed_rdfmts = {}
        # for each matching RDFMT, get corresponding RML TripleMaps
        for mtID, mtpreds in star_rdfmts.items():
            rdfmt = self.config.rdfmts_obj[mtID]
            # get TripleMap IDs
            mappingIds = rdfmt.mappingIds[self.datasource.dsId]
            # for each TripleMap, translate query based on mapping rules
            for tmId, rdf_types in mappingIds.items():
                if mtID in processed_rdfmts and tmId in processed_rdfmts[mtID]:
                    continue
                processed_already = False
                for typ in rdf_types:
                    if typ in processed_rdfmts and tmId in processed_rdfmts[typ]:
                        # mark all processed
                        mts_processed = {ty: [tmId] for ty in rdf_types}
                        processed_rdfmts.update(mts_processed)
                        processed_already = True
                # triple map is already processed via another MT, then go to the next triple map
                if processed_already:
                    continue
                tm = source.mappings[tmId]
                star_query = self.translate_const_predicates(tm, star['triples'], star['variables'])
                sparql_result_templates.update(star_query.sparql_result_template)
                triple_map_queries.append(star_query)
                for typ in rdf_types:
                    processed_rdfmts.setdefault(typ, []).append(tmId)
        missing_predicate_queries = []
        complete_matching_queries = []
        for q in triple_map_queries:
            if len(q.missing_predicates) > 0:
                # print(q.missing_predicates)
                missing_predicate_queries.append((q.missing_predicates, q))
            else:
                complete_matching_queries.append(q)

        for missing_preds, query in missing_predicate_queries:
            missing_triple_patterns = [triple_pattern for triple_pattern in star['triples'] if SPARQL.getUri(triple_pattern.predicate, self.prefixes)[1:-1] in missing_preds]
            variables = []
            for tp in missing_triple_patterns:
                variables.extend(tp.getVars())
            subject_variable = missing_triple_patterns[0].subject.name[1:]

            base_variables = [se.alias for se in query.star_query[0].select_exprs if '?' + se.alias in variables]
            base_variables.append(subject_variable)
            base_variables = list(set(base_variables))
            unions_join_queries = []
            for comp_query in complete_matching_queries:
                join_query = TripleMap2SQL(comp_query.triple_map, missing_triple_patterns, self.service.filters, variables, self.rml, self.prefixes)
                unions_join_queries.append(join_query.star_query)
            unions = SQLUnions(unions_join_queries)
            base_query_alias = SQLTranslator.get_global_table_counter()
            join_query_alias = SQLTranslator.get_global_table_counter()
            join_conditions = SQLCondition(SQLColumn('`' + subject_variable + '`', base_query_alias), '=',
                                           SQLColumn('`' + subject_variable + '`', join_query_alias))

            projections = [SQLSelectExpression(SQLColumn('`' + v[1:] + '`'))
                           for v in star['variables'] if v[1:] not in base_variables]
            projections.extend([SQLSelectExpression(SQLColumn('`' + variable + '`', base_query_alias), variable)
                                for variable in base_variables])
            join_query = SQLJoins(query, {join_query_alias: unions}, join_conditions, base_query_alias, projections )
            complete_matching_queries.append(join_query)

        if len(complete_matching_queries) > 1:
            star_query = SQLUnions(complete_matching_queries)
        elif len(complete_matching_queries) == 1:
            star_query = complete_matching_queries[0]
        else:
            star_query = None
        # print("========Star_Query===========")
        # print(star_query)

        return star_query, sparql_result_templates

    def translate_const_predicates(self, triple_map, triple_patterns: List, variables: List[str]):
        """
        Given a set of triple patterns with constant predicates, find matching predicates and translate them to SQL.
        ``` For all matching preds:
            SELECT pred_obj.col as objVar, ...
            FROM logicalsource.table/qeury
            WHERE subj_map.columns is not Null and not empty and object_map.columns not null and not empty
            ```
        Note: this is considered as conjunctive query matching all triple patterns in the given list.
        Thats why we need to check all columns in subject and object terms are not null or empty.

        :param triple_map:
        :param triple_patterns: List of SPARQL triple patterns
        :param prefixes:  dict of prefixed as {rdf: http://...}
        :return:
        """
        # Formulate filter_conditions from sparql FILTER statements
        query_filters = [f for f in self.service.filters if isinstance(f, Filter)]

        # Find projections and bound constrained columns
        # (i.e., being not NULL or not empty string)
        # for predicates and object in each triple patterns in a star shaped sub-query
        star_query = TripleMap2SQL(triple_map, triple_patterns, query_filters, variables, self.rml, self.prefixes)

        return star_query

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

        # Formulate filter_conditions from sparql FILTER statements
        query_filters = [f for f in self.service.filters if isinstance(f, Filter)]

        # Find projections and bound constrained columns
        # (i.e., being not NULL or not empty string)
        # for predicates and object in each triple patterns in a star shaped sub-query
        # print('=0=-0=-0=-0=-0-=0=-0=-0=-0=-0=-')
        # print(constant_vars, variable_vars, constant_tps)
        star_query = TripleMap2SQL(triple_map, constant_tps+variable_tps, query_filters, constant_vars+variable_vars, self.rml, self.prefixes)

        return star_query

    def translate_subject(self, triple_map, rdf_term):
        subject_map = triple_map.subject_map.subject
        subject_term = rdf_term
        sql = SQLSelectQuery(True)

        term_2_sql = TermMap2SQL(subject_map, subject_term)

        if term_2_sql.projection:
            sql.select_exprs.append(term_2_sql.projection)
        if term_2_sql.filter_conditions:
            sql.where_exprs.append(term_2_sql.filter_conditions)

        sql.from_exprs = [LogicalSource2SQL(triple_map.logical_source).from_expr]
        return sql

    @staticmethod
    def get_table_name(logical_source):
        import hashlib
        if logical_source.table_name is not None:
            table_name = logical_source.table_name
        elif logical_source.query is not None:
            table_name = '(' + logical_source.query + ')'
        else:
            table_name = str(hashlib.md5(str(logical_source.source.name).encode()).hexdigest())
        return table_name
