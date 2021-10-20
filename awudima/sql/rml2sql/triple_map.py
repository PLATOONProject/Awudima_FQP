
__author__ = "Kemele M. Endris"

from typing import Dict, List

from awudima.sql.lang.model import SQLUnions, SQLFromExpression, SQLJoins, SQLSelectQuery
from awudima.sql.lang.model import SQLSelectExpression, SQLColumn, SQLCondition, SQLAndCondition
from awudima.sql.rml2sql.logical_source import LogicalSource2SQL
from awudima.sql.rml2sql.term_map import TermMap2SQL
from awudima.sql.rml2sql.object_reference import ObjectReferenceMap2SQL

from awudima.pyrml import TripleMap, ObjectReferenceMap, TermMap, TermType, TripleMapType
from awudima.pysparql import Argument, Triple, Filter, SPARQL


class TripleMap2SQL:
    _global_table_counter = 0

    @staticmethod
    def get_global_table_counter(prefix='table_name_'):
        TripleMap2SQL._global_table_counter += 1
        if prefix is None or len(prefix) == 0:
            prefix = 'table_name_'
        return prefix + str(TripleMap2SQL._global_table_counter)

    def __init__(self,
                 triple_map: TripleMap,
                 triple_patterns: List[Triple],
                 filters: List[Filter],
                 variables: List[str],
                 rml: Dict,
                 prefixes: Dict
                 ):
        self.triple_map = triple_map
        self.triple_patterns = triple_patterns
        self.filters = filters
        self.rml = rml
        self.prefixes = prefixes
        self.variables = variables

        self.child_projections = None
        self.child_filter_conditions = None
        self.subject_query = None
        self.child_relation = None

        self.star_projections = None
        self.star_where_conditions = None
        self.star_from_expressions = None

        self.ref_obj_queries = None

        self.missing_predicates = None
        self.matching_predicates = None

        self.sparql_result_template = {}
        self.variable_predicate_queries = {}
        self.sql = None

        # self._process_triple_map()
        self._translate_triple_map()

    def _update_sparql_result_template(self, sparql_result_template):
        if sparql_result_template is None or not isinstance(sparql_result_template, dict):
            return
        for item in sparql_result_template:
            if len(sparql_result_template[item]) == 0:
                continue
            if item not in self.sparql_result_template:
                self.sparql_result_template[item] = sparql_result_template[item]
            else:
                if sparql_result_template[item]['type'] != self.sparql_result_template[item]['type']:
                    if sparql_result_template[item]['type'] == 'literal':
                        self.sparql_result_template[item] = sparql_result_template[item]

    def _translate_child_subject(self):

        subject_map = self.triple_map.subject_map
        subject_term = self.triple_patterns[0].subject

        subject_query = TermMap2SQL(subject_map.subject, subject_term, comparision_opr='=')
        self.subject_query = subject_query

        sprojections = subject_query.projection
        sfilter_conditions = subject_query.filter_conditions
        if not subject_term.constant:
            ssparql_result_template = {subject_term.name[1:]: subject_query.sparql_result_template}
        else:
            ssparql_result_template = None
        if sprojections is not None:
            if self.child_projections is None:
                self.child_projections = []
            self.child_projections.append(sprojections)

        if sfilter_conditions is not None and isinstance(sfilter_conditions, SQLAndCondition):
            if isinstance(self.child_filter_conditions, SQLAndCondition):
                self.child_filter_conditions.conditions.extend(sfilter_conditions.conditions)
            else:
                self.child_filter_conditions = sfilter_conditions

        if ssparql_result_template and len(ssparql_result_template) > 0:
            self._update_sparql_result_template(ssparql_result_template)

    @staticmethod
    def get_sql_col_select_expr(col_name, alias=None, constant=False):
        if constant:
            column = SQLColumn("'" + col_name + "'")
        else:
            column = SQLColumn("`" + col_name + "`")
        expr = SQLSelectExpression(column, alias)
        # SQLColumn("'" + ",".join(subject_map.rdf_types) + "'")
        # object_term.name[1:]
        return expr

    def _process_term_map(self, term_map, rdf_term, comparison_opr):
        term_query = TermMap2SQL(term_map, rdf_term, comparison_opr)
        self._set_child_projections(term_query.projection)
        self._set_star_projections(term_query.projection)
        self._set_child_filter_conditions(term_query.filter_conditions)
        self._update_sparql_result_template({rdf_term.name[1:]: term_query.sparql_result_template})

    def _process_same_source_parent_map(self, ref_obj_map, parent_map, rdf_term, comparison_opr):

        self._process_term_map(parent_map.subject_map.subject, rdf_term, comparison_opr)

        join_conditions = []
        for child_column, parent_column in ref_obj_map.join_conditions:
            child_column = SQLColumn(child_column)
            parent_column = SQLColumn(parent_column)
            join_condition = SQLCondition(child_column, '=', parent_column)
            join_conditions.append(join_condition)

        if len(join_conditions) > 0:
            self._set_child_filter_conditions(SQLAndCondition(join_conditions))

    def _process_parent_map_joins(self, ref_obj_map, parent_map, rdf_term, comparison_opr):
        object_query = ObjectReferenceMap2SQL(ref_obj_map, parent_map, rdf_term, comparison_opr)

        star_projs = [SQLColumn(p.alias) for p in object_query.ref_sql.select_exprs if p.alias is not None]
        for column in star_projs:
            self._set_star_projections(SQLSelectExpression(column))

        if len(ref_obj_map.child_columns) > 0:
            child_columns_proj = [SQLSelectExpression(SQLColumn(p)) for p in ref_obj_map.child_columns]
            not_null_conditions = SQLAndCondition([SQLCondition(p, ' IS NOT ', 'NULL') for p in child_columns_proj])
            self._set_child_filter_conditions(not_null_conditions)
            for projection in child_columns_proj:
                self._set_child_projections(projection)
                self._set_star_projections(projection)

        self.ref_obj_queries.append(object_query)
        self._update_sparql_result_template({rdf_term.name[1:]: {'type': 'uri', 'value': ''}})

    def _process_ref_obj_map(self, ref_obj_map, rdf_term, comparison_opr):
        parent_map = self.rml[ref_obj_map.parent_map]

        parent_table_name = LogicalSource2SQL.get_table_name(parent_map.logical_source)
        child_table_name = LogicalSource2SQL.get_table_name(self.triple_map.logical_source)

        if parent_table_name == child_table_name:
            self._process_same_source_parent_map(ref_obj_map, parent_map, rdf_term, comparison_opr)
        else:
            self._process_parent_map_joins(ref_obj_map, parent_map, rdf_term, comparison_opr)

    def _translate_const_predicate_triple_pattern(self,
                                                  predicate_term: Argument,  # SPARQL predicate term
                                                  object_term: Argument,  # SPARQL object term
                                                  comparison_oper='='):
        subject_map = self.triple_map.subject_map
        pred_obj_maps = self.triple_map.const_predicate_object_maps

        predicate_term_uri = SPARQL.getUri(predicate_term, self.prefixes)[1:-1]
        if predicate_term_uri not in pred_obj_maps:
            self.missing_predicates.setdefault(predicate_term_uri, []).append(object_term)
        else:
            object_maps = pred_obj_maps[predicate_term_uri]
            for object_map in object_maps:
                if isinstance(object_map.objectt, TermMap):
                    if predicate_term_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':

                        if not object_term.constant:
                            rdf_types_str = ",".join(subject_map.rdf_types)
                            proj_alias = object_term.name[1:]
                            select_expr = TripleMap2SQL.get_sql_col_select_expr(rdf_types_str, alias=proj_alias,
                                                                                constant=True)
                            self.child_projections.append(select_expr)
                            res_template = {proj_alias: {'type': 'uri', 'value': rdf_types_str}}
                            self._update_sparql_result_template(res_template)
                        else:
                            if object_term.name[1:-1] not in subject_map.rdf_types:
                                if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' not in pred_obj_maps:
                                    self._set_child_filter_conditions(SQLAndCondition([SQLCondition(SQLColumn(False),
                                                                                                    '=', True)]))
                                else:
                                    self._process_term_map(object_map.objectt, object_term, comparison_oper)
                    else:
                        # Object maps with TermMap will be treated as just term maps
                        self._process_term_map(object_map.objectt, object_term, comparison_oper)
                        if not object_term.constant:
                            if object_map.objectt.term_type == TermType.Literal:
                                if object_map.language is not None:
                                    self.sparql_result_template[object_term.name[1:]]['xml:lang'] = object_map.language
                                elif object_map.data_type:
                                    self.sparql_result_template[object_term.name[1:]]['datatype'] = object_map.data_type

                    # keep track of matching predicates from the mapping rules
                    self.matching_predicates.setdefault(predicate_term_uri, []).append(object_term)
                else:
                    # if parent triple map is not available in the mapping rules, then record this predicate
                    # as missing predicate from the rules
                    if object_map.objectt.parent_map not in self.rml:
                        self.missing_predicates.setdefault(predicate_term_uri, []).append(object_term)
                        continue
                    # process object reference mapping that references another triple map
                    self._process_ref_obj_map(object_map.objectt, object_term, comparison_oper)
                    # keep track of matching predicates from the mapping rules
                    self.matching_predicates.setdefault(predicate_term_uri, []).append(object_term)

    def get_star_query(self):
        base_query = SQLSelectQuery(True)

        if len(self.missing_predicates) > 0:
            return None

        base_query.select_exprs = self.child_projections
        if self.child_filter_conditions is not None:
            base_query.where_exprs.append(self.child_filter_conditions)
        base_query.from_exprs = [SQLFromExpression(self.child_relation)]

        for sp, var_unions in self.variable_predicate_queries.items():
            projection_vars = var_unions[0]

            base_vars = [p.alias for p in base_query.select_exprs]
            join_vars = list(set(projection_vars).intersection(base_vars))
            # projection_vars = var_unions[0] + base_vars
            # projection_vars = [v for v in projection_vars if v not in join_vars]

            union_query = SQLSelectQuery(True)

            unions = SQLUnions(var_unions[1])
            base_query_alias = TripleMap2SQL.get_global_table_counter('base_query_')
            # sort as sparql_projections, since union queries need to have same sequences of projections
            projection_vars = [SQLSelectExpression(SQLColumn(v)) if v not in join_vars else SQLSelectExpression(
                SQLColumn(v, base_query_alias), v)
                               for v in list(set(var_unions[0] + base_vars))]
            union_query.select_exprs = projection_vars

            union_query.where_exprs = []
            union_query.from_exprs = [SQLFromExpression(base_query, base_query_alias)]
            union_query_alias = TripleMap2SQL.get_global_table_counter('union_query_')
            union_query.from_exprs.append(SQLFromExpression(unions, union_query_alias))
            if len(join_vars) > 0:
                union_query.select_exprs.extend(
                    [SQLSelectExpression(SQLColumn(v, base_query_alias), v) for v in join_vars])
                for v in join_vars:
                    base_column = SQLColumn(v, base_query_alias)
                    union_column = SQLColumn(v, union_query_alias)
                    join_condition = SQLCondition(base_column, '=', union_column)
                    union_query.where_exprs.append(join_condition)
            base_query = union_query
            # print("UNIONs query:\n\n")
            # print(union_query)
            # print('\n\n========')

        if len(self.ref_obj_queries) > 0:
            base_relation_alias = TripleMap2SQL.get_global_table_counter('child_relation_')

            joining_queries = {}
            join_conditions = []
            join_variables = []
            for object_query in self.ref_obj_queries:
                ref_obj_alias = TripleMap2SQL.get_global_table_counter('parent_relation_')
                if isinstance(object_query, tuple):
                    joining_queries[ref_obj_alias] = object_query[1]
                    join_conditions_tpl = object_query[0]
                else:
                    joining_queries[ref_obj_alias] = object_query.ref_sql
                    join_conditions_tpl = object_query.join_conditions
                if len(join_conditions_tpl) > 0:
                    for left, right in join_conditions_tpl:
                        child_column = SQLColumn(left, base_relation_alias)
                        parent_column = SQLColumn(right, ref_obj_alias)
                        join_condition = SQLCondition(child_column, '=', parent_column)
                        join_conditions.append(join_condition)
                        join_variables.append(left)

            projections = [SQLSelectExpression(SQLColumn(v[1:])) for v in self.variables if v[1:] not in join_variables]
            for v in join_variables:
                projections.append(SQLSelectExpression(SQLColumn(v, base_relation_alias), v))

            base_query = SQLJoins(base_query,
                                  joining_queries,
                                  SQLAndCondition(join_conditions),
                                  base_relation_alias,
                                  projections).sql
        self.sql = base_query
        # print("====================================================")
        # print(base_query)
        # print("====================================================")

        return base_query, self.sparql_result_template

    def _get_subject_var(self, operand):
        if operand.name == self.triple_patterns[0].subject.name:
            return self.triple_patterns[0].subject

    def _get_object_var(self, operand):
        predicates = []
        for tp in self.triple_patterns:
            if tp.theobject.name == operand.name:
                predicates.append(tp.predicate)
        return predicates

    def _translate_filter_expression(self, expression):
        where_expressions = []
        if expression.op in SPARQL.unaryFunctor:
            pass
        elif expression.op in SPARQL.binaryFunctor:
            pass
        else:
            if isinstance(expression.left, Argument) and not expression.left.constant:
                if isinstance(expression.right, Argument):
                    subject = self._get_subject_var(expression.left)
                    predicates = self._get_object_var(expression.left)
                    if expression.right.constant:
                        if subject is not None:
                            conds = self._get_term_map_filter_conditions(expression.right,
                                                                         expression,
                                                                         self.triple_map.subject_map.subject)
                            where_expressions.extend(conds)
                        else:
                            conds = self._get_predicate_filter_conditions(expression.right, expression, predicates)
                            where_expressions.extend(conds)
                    else:
                        right_subject = self._get_subject_var(expression.right)
                        right_predicates = self._get_object_var(expression.right)

            if isinstance(expression.right, Argument) and not expression.right.constant:
                subject = self._get_subject_var(expression.right)
                predicates = self._get_object_var(expression.right)
                if isinstance(expression.left, Argument):
                    if expression.left.constant:
                        if subject is not None:
                            conds = self._get_term_map_filter_conditions(expression.left, expression,
                                                                         self.triple_map.subject_map.subject)
                            where_expressions.extend(conds)
                        else:
                            conds = self._get_predicate_filter_conditions(expression.left, expression, predicates)
                            where_expressions.extend(conds)

        return where_expressions

    def _translate_filters(self):
        exprs = []
        for f in self.filters:
            exprs.append(f.expr)

        where_conditions = []
        for e in exprs:
            where_exprs = self._translate_filter_expression(e)
            where_conditions.extend(where_exprs)

        if len(where_conditions) > 0:
            if self.child_filter_conditions is not None:
                self.child_filter_conditions.conditions.extend(where_conditions)
            else:
                self.child_filter_conditions = SQLAndCondition(where_conditions)

    @staticmethod
    def translate_term_map(term_map, rdf_term, comparison_operator):
        """

        :param term_map:
        :param rdf_term:
        :param comparison_operator:

        :return:
        """
        term_query = TermMap2SQL(term_map, rdf_term, comparison_operator)
        projection = term_query.projection
        filter_conditions = term_query.filter_conditions
        if not rdf_term.constant:
            sparql_result_template = {rdf_term.name[1:]: term_query.sparql_result_template}
        else:
            sparql_result_template = dict()

        return projection, filter_conditions, sparql_result_template

    @staticmethod
    def translate_ref_obj_map(rml_map, logical_source, ref_obj_map, rdf_term, comparison_opr='='):
        parent_map = rml_map[ref_obj_map.parent_map]
        child_projections = []
        star_projections = []
        child_filter_conditions = []
        ref_obj_queries = []
        sparql_result_template = {}

        parent_table_name = LogicalSource2SQL.get_table_name(parent_map.logical_source)
        child_table_name = LogicalSource2SQL.get_table_name(logical_source)

        if parent_table_name == child_table_name:
            subject_query = TermMap2SQL(parent_map.subject_map.subject, rdf_term, comparison_opr)
            if subject_query.projection is not None:
                child_projections.append(subject_query.projection)
                star_projections.append(subject_query.projection)

            # if subject_query.bound_columns is not None:
            #     bound_column_conditions.extend(subject_query.bound_columns.conditions)

            if subject_query.sparql_result_template is not None and len(subject_query.sparql_result_template) > 0:
                if not rdf_term.constant:
                    sparql_result_template.update({rdf_term.name[1:]: subject_query.sparql_result_template})

            join_conditions = []
            for child_column, parent_column in ref_obj_map.join_conditions:
                child_column = SQLColumn(child_column)
                parent_column = SQLColumn(parent_column)
                join_condition = SQLCondition(child_column, '=', parent_column)
                join_conditions.append(join_condition)

            if subject_query.filter_conditions is not None:
                child_filter_conditions.extend(subject_query.filter_conditions.conditions)

            if len(join_conditions) > 0:
                child_filter_conditions.extend(join_conditions)
        else:
            object_query = ObjectReferenceMap2SQL(ref_obj_map, parent_map, rdf_term, comparison_opr)

            star_projs = [SQLColumn(p.alias) for p in object_query.ref_sql.select_exprs if p.alias is not None]
            star_projections.extend(star_projs)

            if len(ref_obj_map.child_columns) > 0:
                child_columns_proj = [SQLSelectExpression(SQLColumn(p)) for p in ref_obj_map.child_columns]
                # We also make sure the child_columns are projected from the child_query so that
                # we can use them as join_conditions.
                child_projections.extend(child_columns_proj)

                star_projections.extend(child_columns_proj)

                # We only need to check the child_columns are not NULL values from the child_query side.
                # No need to filter conditions if rdf_term is constant, because this constant filter
                # is pushed down to the object_query (ref_obj_query over the subject_map of the parent_map .
                not_null_conditions = [SQLCondition(p, ' IS NOT ', 'NULL') for p in child_columns_proj]
                child_filter_conditions.extend(not_null_conditions)

            ref_obj_queries.append(object_query)

            if not rdf_term.constant:
                if parent_map.subject_map.subject.term_type == TermType.IRI:
                    sparql_result_template.update({rdf_term.name[1:]: {'type': 'uri', 'value': ''}})
                else:
                    sparql_result_template.update({rdf_term.name[1:]: {'type': 'bnode', 'value': ''}})

        if len(child_filter_conditions) > 0:
            child_filter_conditions = SQLAndCondition(child_filter_conditions)
        else:
            child_filter_conditions = None
        return child_projections, star_projections, \
               child_filter_conditions, ref_obj_queries, sparql_result_template

    @staticmethod
    def translate_object_maps(predicate_term: Argument,
                              predicate_term_uri: str,
                              object_term: Argument,
                              subject_map,
                              object_maps,
                              rml,
                              logical_source,
                              comparison_opr='='):
        projections = []
        filter_conditions = None
        sparql_result_template = {}
        ref_obj_map_queries = []
        missing_predicates = {}
        sparql_projections = []

        for object_map in object_maps:
            if isinstance(object_map.objectt, TermMap):
                if predicate_term_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' and \
                        not object_term.constant:
                    projections.append(
                        SQLSelectExpression(SQLColumn("'" + ",".join(subject_map.rdf_types) + "'"),
                                            object_term.name[1:]))
                    if not predicate_term.constant:
                        projections.append(
                            SQLSelectExpression(SQLColumn("'" + predicate_term_uri + "'"), predicate_term.name[1:]))
                        sparql_result_template.update({predicate_term.name[1:]: {'type': 'uri', 'value': ''}})
                    sparql_result_template.update(
                        {object_term.name[1:]: {'type': 'uri', 'value': subject_map.rdf_types}})
                else:
                    # Object maps with TermMap will be treated as just term maps
                    # self._process_term_map(object_map.objectt, object_term, comparison_opr)

                    cprojections, cfilter_conditions, \
                    csparql_result_template = TripleMap2SQL.translate_term_map(object_map.objectt, object_term,
                                                                               comparison_opr)
                    if not object_term.constant:
                        sparql_projections.append(object_term.name[1:])

                    if cprojections is not None:
                        projections.append(cprojections)
                    if not predicate_term.constant:
                        projections.append(
                            SQLSelectExpression(SQLColumn("'" + predicate_term_uri + "'"), predicate_term.name[1:]))
                        sparql_result_template.update({predicate_term.name[1:]: {'type': 'uri', 'value': ''}})

                    if cfilter_conditions is not None:
                        if filter_conditions is not None:
                            filter_conditions.conditions.extend(cfilter_conditions.conditions)
                        else:
                            filter_conditions = cfilter_conditions

                    sparql_result_template.update(csparql_result_template)

                    if (
                            object_map.objectt.term_type is not None and object_map.objectt.term_type == TermType.Literal) or \
                            object_map.objectt.resource_type != TripleMapType.TEMPLATE:
                        # TODO: implement also languageMaps not only constant language terms
                        if object_map.language is not None:
                            sparql_result_template.update(
                                {object_term.name[1:]: {'type': 'literal', 'value': '',
                                                        'xml:lang': str(object_map.language)}})
                        if object_map.data_type is not None:
                            sparql_result_template.update(
                                {object_term.name[1:]: {'type': 'literal', 'value': '',
                                                        'datatype': str(object_map.data_type)}})

            else:
                # if parent triple map is not available in the mapping rules, then record this predicate
                # as missing predicate from the rules
                if object_map.objectt.parent_map not in rml:
                    missing_predicates.setdefault(predicate_term_uri, []).append(object_term)
                    continue
                # process object reference mapping that references another triple map
                # self._process_ref_obj_map(object_map.objectt, object_term, comparison_opr) rml, logical_source, ref_obj_map, rdf_term, comparison_opr
                vprojections, vstar_projections, \
                vfilter_conditions, vref_obj_queries, \
                vsparql_result_template = TripleMap2SQL.translate_ref_obj_map(rml,
                                                                              logical_source,
                                                                              object_map.objectt,
                                                                              object_term,
                                                                              comparison_opr)
                # for i in range(len(vprojections)):
                #     projections[object_term.name + str(i)] = vprojections
                if len(vprojections) > 0:
                    projections.extend(vprojections)

                if not object_term.constant:
                    sparql_projections.append(object_term.name[1:])

                if vfilter_conditions is not None:
                    if filter_conditions is not None:
                        filter_conditions.conditions.extend(vfilter_conditions.conditions)
                    else:
                        filter_conditions = vfilter_conditions

                sparql_result_template.update(vsparql_result_template)
                ref_obj_map_queries.extend(vref_obj_queries)

        return projections, filter_conditions, ref_obj_map_queries, \
               sparql_result_template, sparql_projections, missing_predicates

    def _reset_lists(self):
        self.child_projections = []
        self.child_filter_conditions = None
        self.child_relation = None

        self.star_projections = []
        self.star_where_conditions = []
        self.star_from_expressions = []

        self.ref_obj_queries = None

        self.missing_predicates = None
        self.matching_predicates = None

        self.star_projections = []
        self.star_where_conditions = []
        self.star_from_expressions = []

        self.ref_obj_queries = []
        self.missing_predicates = {}
        self.matching_predicates = {}

        self.sparql_result_template = {}
        self.variable_predicate_queries = {}

    def _set_child_filter_conditions(self, filter_conditions):
        if filter_conditions is not None and isinstance(filter_conditions, SQLAndCondition):
            if self.child_filter_conditions is not None:
                self.child_filter_conditions.conditions.extend(filter_conditions.conditions)
            else:
                self.child_filter_conditions = filter_conditions

    def _set_child_projections(self, projection):
        if projection is not None and isinstance(projection, SQLSelectExpression):
            if self.child_projections is None:
                self.child_projections = []
            self.child_projections.append(projection)

    def _set_star_projections(self, projection):
        if projection is not None and isinstance(projection, SQLSelectExpression):
            if self.star_projections is None:
                self.star_projections = []
            self.star_projections.append(projection)

    def translate_predicate_object_map(self,
                                       subject_term: Argument,  # SPARQL subject term
                                       predicate_term: Argument,  # SPARQL predicate term
                                       object_term: Argument,  # SPARQL object term
                                       # subject_query: TermMap2SQL,  # translated SQL query of the subject term
                                       comparison_operator='='):
        sparql_result_template = {}
        missing_predicates = {}
        union_queries = {}
        subject_map = self.triple_map.subject_map
        pred_obj_maps = self.triple_map.const_predicate_object_maps

        if predicate_term.constant:
            predicate_term_uri = SPARQL.getUri(predicate_term, self.prefixes)[1:-1]
            if predicate_term_uri not in pred_obj_maps:
                object_maps = []
            else:
                object_maps = pred_obj_maps[predicate_term_uri]
            return TripleMap2SQL.translate_object_maps(predicate_term,
                                                       predicate_term_uri,
                                                       object_term,
                                                       subject_map,
                                                       object_maps,
                                                       self.rml,
                                                       self.triple_map.logical_source,
                                                       comparison_operator)
        else:

            # Note predicateMaps are always assumed to be constants; neither TEMPLATE nor REFERENCE
            # predicate_map_uri is a constant IRI of RDF term
            for predicate_map_uri, object_maps in pred_obj_maps.items():
                rprojections, rfilter_conditions, rref_obj_map_queries, \
                rsparql_result_template, rsparql_projections, rmissing_predicates \
                = TripleMap2SQL.translate_object_maps(predicate_term,
                                                      predicate_map_uri,
                                                      object_term,
                                                      subject_map,
                                                      object_maps,
                                                      self.rml,
                                                      self.triple_map.logical_source,
                                                      comparison_operator)

                # 0. Include subject query
                if rprojections is not None:
                    rprojections.append(self.subject_query.projection)
                if rfilter_conditions is not None and self.subject_query.filter_conditions is not None:
                    rfilter_conditions.conditions.extend(self.subject_query.filter_conditions.conditions)
                if rsparql_projections is not None and not subject_term.constant:
                    rsparql_projections.append(subject_term.name[1:])
                # 1. create a SQLQuery
                var_query = TripleMap2SQL.create_sql_query(self.child_relation, rprojections,
                                                           rfilter_conditions, rref_obj_map_queries,
                                                           rsparql_projections)
                # 2. append to unions
                union_queries[subject_term.name + predicate_term.name + object_term.name+predicate_map_uri] = var_query

                sparql_result_template.update(rsparql_result_template)
                missing_predicates.update(rmissing_predicates)
            return union_queries, sparql_result_template, missing_predicates

    # translate
    def _translate_triple_map(self):
        # reset list properties, so that we start fresh analyzing this triple map query
        self._reset_lists()
        sparql_result_template = {}

        ref_obj_map_queries = []
        missing_predicates = {}

        union_queries = {}

        variable_preds = {}
        # child relation is the base relation for this triple maps' logical source
        self.child_relation = LogicalSource2SQL(self.triple_map.logical_source).from_expr
        self._translate_filters()

        # Step 1: translate the subject map
        self._translate_child_subject()

        # Step 2: translate the object maps
        subject_term = self.triple_patterns[0].subject
        for triple_pattern in self.triple_patterns:
            # predicate term of a triple pattern to translate
            predicate_term = triple_pattern.predicate
            # object term of a triple pattern to translate
            object_term = triple_pattern.theobject
            if predicate_term.constant:
                self._translate_const_predicate_triple_pattern(predicate_term, object_term, '=')
            else:
                runion_queries, rsparql_result_template, rmissing_predicates = self.translate_predicate_object_map(
                    subject_term,
                    predicate_term,
                    object_term,
                    '=')
                if len(missing_predicates) == 0:
                    union_queries.update(runion_queries)
                    if rsparql_result_template and len(rsparql_result_template) > 0:
                        sparql_result_template.update(rsparql_result_template)
                    missing_predicates.update(rmissing_predicates)
                    if not subject_term.constant:
                        variable_preds[subject_term.name + predicate_term.name + object_term.name] = triple_pattern

        if len(union_queries) > 0:
            ref_obj_map_queries.append(
                ([(subject_term.name[1:], subject_term.name[1:])], SQLUnions(list(union_queries.values()))))

        self.get_star_query()

    @staticmethod
    def create_sql_query(relation,
                         projections,
                         filter_conditions,
                         ref_obj_map_queries,
                         sparql_projection):

        base_query = SQLSelectQuery(True)

        base_query.select_exprs = projections
        if filter_conditions is not None:
            base_query.where_exprs.append(filter_conditions)
        base_query.from_exprs = [SQLFromExpression(relation)]

        if len(ref_obj_map_queries) > 0:
            base_relation_alias = TripleMap2SQL.get_global_table_counter('child_relation_')
            joining_queries = {}
            join_conditions = []
            join_variables = []
            for object_query in ref_obj_map_queries:
                ref_obj_alias = TripleMap2SQL.get_global_table_counter('parent_relation_')
                if isinstance(object_query, tuple):
                    joining_queries[ref_obj_alias] = object_query[1]
                    join_conditions_tpl = object_query[0]
                else:
                    joining_queries[ref_obj_alias] = object_query.ref_sql
                    join_conditions_tpl = object_query.join_conditions

                if len(join_conditions_tpl) > 0:
                    for left, right in join_conditions_tpl:
                        child_column = SQLColumn(left, base_relation_alias)
                        parent_column = SQLColumn(right, ref_obj_alias)
                        join_condition = SQLCondition(child_column, '=', parent_column)
                        join_conditions.append(join_condition)
                        join_variables.append(left)

            # projections = [SQLSelectExpression(SQLColumn(v[1:]))
            # for v in self.variables if v[1:] not in join_variables]
            for v in sparql_projection:
                projections.append(SQLSelectExpression(SQLColumn(v)))
            for v in join_variables:
                projections.append(SQLSelectExpression(SQLColumn(v, base_relation_alias), v))

            base_query = SQLJoins(base_query,
                                  joining_queries,
                                  SQLAndCondition(join_conditions),
                                  base_relation_alias,
                                  projections).sql

        # if len(ref_obj_map_queries) > 0:
        #     star_query = SQLQuery(True)
        #     base_relation_alias = TripleMap2SQL.get_global_table_counter('child_relation_')
        #     star_query.select_exprs = []
        #     for v in sparql_projection:
        #         star_query.select_exprs.append(SQLSelectExpression(SQLColumn(v)))
        #
        #     star_query.where_exprs = []
        #     star_query.from_exprs = [SQLFromExpression(base_query, base_relation_alias)]
        #     for object_query in ref_obj_map_queries:
        #         ref_obj_alias = TripleMap2SQL.get_global_table_counter('parent_relation_')
        #         ref_relation = SQLFromExpression(object_query.sql, ref_obj_alias)
        #         star_query.from_exprs.append(ref_relation)
        #         if len(object_query.join_conditions) > 0:
        #             for left, right in object_query.join_conditions:
        #                 child_column = SQLColumn(left, base_relation_alias)
        #                 parent_column = SQLColumn(right, ref_obj_alias)
        #                 join_condition = SQLCondition(child_column, '=', parent_column)
        #                 star_query.where_exprs.append(join_condition)

        # base_query = star_query

        return base_query

    def _compare_term_maps(self, left_term, expression, right_term):
        where_expressions = []
        term_left, left_sparql_result_template = TermMap2SQL.get_sql_term(left_term)
        term_right, right_sparql_result_template = TermMap2SQL.get_sql_term(right_term)
        if term_left is not None and term_right is not None:
            cond = SQLCondition(term_left, expression.op, term_right)
            where_expressions.append(cond)
        else:
            self.missing_predicates.setdefault(str(expression), []).append(
                expression)

        return where_expressions

    def _get_term_map_filter_conditions(self, expression_arg, expression, term_map):
        where_expressions = []
        term, sparql_result_template = TermMap2SQL.get_sql_term(term_map)
        if term is not None:
            cond = SQLCondition(term, expression.op, expression_arg)
            where_expressions.append(cond)
        else:
            self.missing_predicates.setdefault(str(expression), []).append(
                expression)

        return where_expressions

    def _get_predicate_filter_conditions(self, expression_arg, expression, predicates):
        where_expressions = []
        for predicate in predicates:
            predicate_term_uri = SPARQL.getUri(predicate, self.prefixes)[1:-1]
            pred_obj_maps = self.triple_map.const_predicate_object_maps
            if predicate_term_uri in pred_obj_maps:
                object_maps = pred_obj_maps[predicate_term_uri]
                for object_map in object_maps:
                    if isinstance(object_map.objectt, TermMap):
                        conds = self._get_term_map_filter_conditions(expression_arg, expression, object_map.objectt)
                        where_expressions.extend(conds)
                    else:
                        if object_map.objectt.parent_map not in self.rml:
                            self.missing_predicates.setdefault(predicate_term_uri, []).append(expression)
                            continue
                        parent_map = self.rml[object_map.objectt.parent_map]
                        parent_table_name = LogicalSource2SQL.get_table_name(parent_map.logical_source)
                        child_table_name = LogicalSource2SQL.get_table_name(self.triple_map.logical_source)

                        if parent_table_name == child_table_name:
                            conds = self._get_term_map_filter_conditions(expression_arg, expression,
                                                                         parent_map.subject_map.subject)
                            where_expressions.extend(conds)
                        else:
                            self._process_parent_map_joins(object_map.objectt, parent_map, expression_arg,
                                                           expression.op)
            else:
                self.missing_predicates.setdefault(predicate_term_uri, []).append(expression)
        return where_expressions

    @property
    def star_query(self):
        return self.sql, self.sparql_result_template

    def __str__(self):
        return str(self.sql)

    def __repr__(self):
        return self.__str__()

    def get_sql(self):
        return self.__str__()


if __name__ == '__main__':
    from pyrml import TermMap, PredicateMap, ObjectMap, TermType, SubjectMap, TripleMapType, RMLSource, LogicalSource, TripleMap, DataSourceType

    # Data source desc
    s = RMLSource("mysqlsource",
                  ds_desc={"http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN": "dbname",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#username": "root",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#password": "mypassword"},
                  dstype=DataSourceType.MYSQL, dbmstype='MySQL')
    # Logical source mapping
    ls = LogicalSource(s, 'row', reference_formulation="MySQL")
    ls.table_name = "sometable"
    # ls.query = "SELECT * FROM sometable "
    # pprint(ls.to_json())
    tmcty = TermMap('http://hello.us/City/{name}', TripleMapType.TEMPLATE, TermType.IRI)
    tmcry = TermMap('http://hello.us/Country/{name}', TripleMapType.TEMPLATE, TermType.IRI)
    cmap = SubjectMap("_:lksdjflsd", tmcty, rdf_types=['http://ontology.com/City', 'http://schema.org/City'])
    pmap = SubjectMap("_:lajdlfsdf", tmcry, rdf_types=['http://ontology.com/Country'])

    pred = TermMap('http://hello.us/name', TripleMapType.CONSTANT, TermType.IRI)
    prm = PredicateMap("_:kjdhfkja", pred)

    obj = TermMap('name', TripleMapType.REFERENCE, TermType.Literal)
    objm = ObjectMap("_:lksdajfer", obj, language='en')
    pom = [(prm, objm)]

    parent_triplemap = TripleMap('http://triplemaps.map/CountryMapping', ls, pmap, pom)

    obref = ObjectReferenceMap(parent_triplemap.uri, [], termtype=TripleMapType.TRIPLEMAP)
    objrefmap = ObjectMap('_:ljdalsdkf', obref)

    cpred = TermMap('http://hello.us/country', TripleMapType.CONSTANT, TermType.IRI)
    cprm = PredicateMap("_:kjdhfkja", cpred)
    opom = [(cprm, objrefmap)]

    triplemap = TripleMap('http://triplemaps.map/CityMapping', ls, cmap, opom)

    sub = Argument("?city", False)
    pred = Argument('<http://hello.us/name>', True)
    obj = Argument('"Hannover"', True)
    tp1 = Triple(sub, pred, obj)
    print(tp1)

    sub = Argument("?city", False)
    pred = Argument('<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', True)
    obj = Argument('<http://ontology.com/Country>', True)
    tp2 = Triple(sub, pred, obj)
    print(tp2)

    from pysparql import Filter, Expression
    f = Filter(Expression('=', Argument('?name', False), Argument('"Hannover"', True)))
    rmlm = {
        parent_triplemap.uri: parent_triplemap,
        triplemap.uri: triplemap
    }
    tmq = TripleMap2SQL(parent_triplemap, [tp1, tp2], [], ['?city'], rmlm, {})
    print(tmq.sql)
