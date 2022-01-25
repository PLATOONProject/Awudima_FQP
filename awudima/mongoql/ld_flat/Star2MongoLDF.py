__author__ = "Kemele M. Endris"

from flask import g
from typing import Dict, List
from awudima.pysparql import Argument, Expression, Triple, Filter, SPARQL
from awudima.pyrdfmt import DataSource


try:
    __globals__ = g.collection
except RuntimeError as e:
    class Global:
        collection = None

    g = Global()


class Star2MongoLDF:

    def __init__(self,
                 rdfmt,
                 triple_patterns: List[Triple],
                 filters: List[Filter],
                 variables: List[str],
                 prefixes: Dict,
                 datasource: DataSource
                 ):
        self.rdfmt = rdfmt
        self.rdfmtID = self.rdfmt.mtId
        self.triple_patterns = triple_patterns
        self.filters = filters
        self.prefixes = prefixes
        self.variables = variables
        self.datasource = datasource

        self.sparql_result_template = {}
        self.mongo_ql = self.translate_to_mongo_ld_flat()

    def url_to_prefixed_vals(self, url, collection_name):
        candidates = []
        for p, v in self.datasource.params[collection_name]['prefixes'].items():
            if v in url:
                candidates.append(url.replace(v, p+':'))
        finalists = []
        for c in candidates:
            if '#' in c or '/' in c:
                pass
            else:
                finalists.append(c)
        return finalists

    def _translate_filters(self, triple_patterns, collection):
        exprs = []
        for f in self.filters:
            exprs.append(f.expr)

        where_conditions = []
        for e in exprs:
            where_exprs = self._translate_filter_expression(e, triple_patterns, collection)
            where_conditions.extend(where_exprs)

        # print(exprs)
        # print(where_conditions)

        return where_conditions

    def _get_subject_var(self, operand, triple_patterns):
        if operand.name == triple_patterns[0].subject.name:
            return triple_patterns[0].subject

    def _get_object_var(self, operand, triple_patterns):
        predicates = []
        for tp in triple_patterns:
            if tp.theobject.name == operand.name:
                predicates.append(tp.predicate)
        return predicates

    def _translate_filter_expression(self, expression, triple_patterns, collection):
        where_expressions = []
        if expression.op in SPARQL.unaryFunctor:
            if expression.op == 'xsd:dateTime':
                # print(expression.op, expression.left.name)
                if isinstance(expression.left, Argument):
                    if not expression.left.constant:
                        return {
                            "$addFields": {
                                expression.left.name[1:]: {"$toDate": expression.left.name[1:]}
                                }
                            }
                    else:
                        expression.left.name = expression.left.name[1:-1] if expression.left.name[0] in ["'", '"'] else expression.left.name
                        return {
                             "$dateFromString": {
                                "dateString": expression.left.name
                                 }
                            }

            else:
                return []
        elif expression.op in SPARQL.binaryFunctor:
            return []
        else:
            if isinstance(expression.left, Expression) and isinstance(expression.right, Expression):
                left_stages = self._translate_filter_expression(expression.left, triple_patterns, collection)
                right_stages = self._translate_filter_expression(expression.right, triple_patterns, collection)
                stages = left_stages[:-1]
                stages.extend(right_stages[:-1])
                operation = None
                if expression.op == '&&':
                    operation = '$and'
                elif expression.op == '||':
                    operation = '$or'
                if operation is not None:
                    matching = {'$match': {
                        '$expr': {operation: [left_stages[-1]['$match']['$expr'], right_stages[-1]['$match']['$expr']]}}}
                    stages.append(matching)
                    return stages
                else:
                    return []
            if isinstance(expression.left, Argument) and not expression.left.constant:
                if isinstance(expression.right, Argument):
                    subject = self._get_subject_var(expression.left, triple_patterns)
                    predicates = self._get_object_var(expression.left, triple_patterns)
                    if expression.right.constant:
                        if subject is not None:
                            conds = self._get_term_map_filter_conditions(expression.right, expression,
                                                                         '$@graph.@id', expression.left.name[1:], collection)
                            where_expressions.extend(conds)
                        else:
                            conds = self._get_predicate_filter_conditions(expression.right, expression, predicates, expression.left.name[1:], collection)
                            where_expressions.extend(conds)
                    else:
                        right_subject = self._get_subject_var(expression.right, triple_patterns)
                        right_predicates = self._get_object_var(expression.right, triple_patterns)
                elif isinstance(expression.right, Expression):
                    right_conds = self._translate_filter_expression(expression.right, triple_patterns, collection)
                    subject = self._get_subject_var(expression.left, triple_patterns)
                    predicates = self._get_object_var(expression.left, triple_patterns)
                    if subject is not None:
                        conds = self._get_term_map_filter_conditions(right_conds, expression, '$@graph.@id', expression.left.name[1:], collection)
                        where_expressions.extend(conds)
                    else:
                        variable = expression.left.name[1:] + str(hash(expression.right.left.name))
                        conds = self._get_predicate_filter_conditions(right_conds, expression, predicates, variable, collection)
                        where_expressions.extend(conds)
            if isinstance(expression.right, Argument) and not expression.right.constant:
                subject = self._get_subject_var(expression.right, triple_patterns)
                predicates = self._get_object_var(expression.right, triple_patterns)
                if isinstance(expression.left, Argument):
                    if expression.left.constant:
                        if subject is not None:
                            conds = self._get_term_map_filter_conditions(expression.left, expression,
                                                                         '$@graph.@id', expression.right.name, collection)
                            where_expressions.extend(conds)
                        else:
                            conds = self._get_predicate_filter_conditions(expression.left, expression, predicates, expression.right.name[1:], collection)
                            where_expressions.extend(conds)

        return where_expressions

    def _get_term_map_filter_conditions(self, expression_arg, expression, term, variable, collection):
        where_expressions = []
        if term is not None:
            cond = (term, expression.op, expression_arg)
            where_expressions.append(cond)

        return where_expressions

    def _get_predicate_filter_conditions(self, expression_arg, expression, predicates, variable, collection):
        where_expressions = []
        rml_source = self.rdfmt.predicate_RML_sources
        for predicate in predicates:
            predicate_term_uri = SPARQL.getUri(predicate, self.prefixes)[1:-1]
            if predicate_term_uri in rml_source:
                psources = rml_source[predicate_term_uri]
                for s in psources:
                    if s.dstype.value == 'MONGODB_LD_FLAT' and s.name.split('<|>')[0] == self.datasource.dsId:
                        if collection == s.ds_desc['collection_name']:
                            if '$dateFromString' in expression_arg:
                                conv_stage = {'$addFields': {'date_conv_' + variable: expression_arg}}
                                predid = self.datasource.params[collection]['predicates'][predicate_term_uri]['@id']
                                comp_stage = {
                                    "$addFields": {
                                        variable + '_value': {"$toDate": '$@graph.' + predid}
                                        }
                                    }
                                operation = ""
                                if expression.op == '=':
                                    operation = '$eq'
                                elif expression.op == ">":
                                    operation = '$gt'
                                elif expression.op == ">=":
                                    operation = '$gte'
                                elif expression.op == "<":
                                    operation = '$lt'
                                elif expression.op == "<=":
                                    operation = '$lte'
                                elif expression.op == "!=":
                                    operation = '$ne'
                                match_stage = {'$match': {'$expr': {operation: ['$'+variable + '_value', '$date_conv_' + variable]}}}
#                                print(conv_stage)
#                                print(comp_stage)
#                                print(match_stage)
                                where_expressions.append(conv_stage)
                                where_expressions.append(comp_stage)
                                where_expressions.append(match_stage)
        return where_expressions

    def translate_to_mongo_ld_flat(self):
        queries_per_collection = {}
        first = True
        # triple patterns per collection
        tps_per_collection = {}
        # collections per triple pattern
        collection_per_tp = {}

        collections = []
        rml_source = self.rdfmt.predicate_RML_sources
        for tp in self.triple_patterns:
            pred = tp.predicate
            pred_uri = SPARQL.getUri(pred, self.prefixes)[1:-1]
            if pred_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                colls = []
                for p in rml_source:
                    for s in rml_source[p]:
                        collection = s.ds_desc['collection_name']
                        if g.collection and not g.collection == collection:
                            continue
                        if s.dstype.value == 'MONGODB_LD_FLAT' and s.name.split('<|>')[0] == self.datasource.dsId:
                            if collection in colls:
                                continue
                            colls.append(collection)
                            tps_per_collection.setdefault(collection, []).append(tp)
                            collection_per_tp.setdefault(str(tp), []).append(collection)
                            collection_per_tp[str(tp)] = list(set(collection_per_tp[str(tp)]))
                            if collection not in collections:
                                collections.append(collection)
                continue
            psources = rml_source[pred_uri]
            for s in psources:
                if s.dstype.value == 'MONGODB_LD_FLAT' and s.name.split('<|>')[0] == self.datasource.dsId:
                    collection = s.ds_desc['collection_name']
                    if g.collection and not g.collection == collection:
                        continue
                    tps_per_collection.setdefault(collection, []).append(tp)
                    collection_per_tp.setdefault(str(tp), []).append(collection)
                    collection_per_tp[str(tp)] = list(set(collection_per_tp[str(tp)]))
                    if collection not in collections:
                        collections.append(collection)

        full_matches = []
        for collection, tps in tps_per_collection.items():
            sparql_result_template = {}
            projections = {'_id': 0}
            and_matches = {'@graph.@type': {'$in': self.url_to_prefixed_vals(self.rdfmtID, collection)}}
            for triple_pattern in tps:
                pred = triple_pattern.predicate
                obj = triple_pattern.theobject
                pred_uri = SPARQL.getUri(pred, self.prefixes)[1:-1]
                if pred_uri not in self.datasource.params[collection]['predicates']:
                    # lookup from another collection and unwind it
                    if pred_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                        subj = triple_pattern.subject
                        if not subj.constant:
                            projections[subj.name[1:]] = '$@graph.@id'
                            sparql_result_template[subj.name[1:]] = {'type': 'uri', 'value': '', 'source': [self.datasource.dsId + '<|>' + collection]}
                        else:
                            and_matches['@graph.@id'] = SPARQL.getUri(subj, self.prefixes)[1:-1]
                    continue
                predid = self.datasource.params[collection]['predicates'][pred_uri]['@id']

                if first:
                    subj = triple_pattern.subject
                    if not subj.constant:
                        projections[subj.name[1:]] = '$@graph.@id'
                        sparql_result_template[subj.name[1:]] = {'type': 'uri', 'value': '', 'source': [self.datasource.dsId + '<|>' + collection]}
                    else:
                        and_matches['@graph.@id'] = SPARQL.getUri(subj, self.prefixes)[1:-1]

                    first = False

                if pred_uri == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                    if not obj.constant:
                        proj_alias = obj.name[1:]
                        projections[proj_alias] = self.rdfmtID
                        sparql_result_template[proj_alias] = {'type': 'uri', 'value': self.rdfmtID, 'source': [self.datasource.dsId + '<|>' + collection]}
                    else:
                        obj_url = SPARQL.getUri(obj, self.prefixes)[1:-1]
                        and_matches['@graph.@type'] = {'$in': self.url_to_prefixed_vals(obj_url, collection)}
                else:
                    if not obj.constant:
                        proj_alias = obj.name[1:]
                        projections[proj_alias] = '$@graph.' + predid
                        if '@type' in self.datasource.params[collection]['predicates'][pred_uri]:
                            tp = self.datasource.params[collection]['predicates'][pred_uri]['@type']
                            if tp == '@id':
                                sparql_result_template[proj_alias] = {'type': 'uri', 'value': '', 'source': [self.datasource.dsId + '<|>' + collection]}
                            else:
                                sparql_result_template[proj_alias] = {'type': 'literal', 'value': '',
                                                             'datatype': tp, 'source': [self.datasource.dsId + '<|>' + collection]}
                        else:
                            sparql_result_template[proj_alias] = {'type': 'literal', 'value': '', 'source': [self.datasource.dsId + '<|>' + collection]}

                        if '$expr' in and_matches:
                            and_matches['$expr']['$and'].extend(
                                                    [{'$ne': ['$@graph.'+predid, 'null']},
                                                     {'$ne': ['$@graph.'+predid, '']}])
                        else:
                            and_matches['$expr'] = {
                                    '$and': [{'$ne': ['$@graph.'+predid, 'null']},
                                             {'$ne': ['$@graph.'+predid, '']}]
                                }
                    else:
                        and_matches['@graph.' + predid] = obj.name[1:-1]
            # print(collection)
            # print({'$match': and_matches})
            # print({'$project': projections})
            # print(sparql_result_template)
            if len(tps) == len(self.triple_patterns):
                full_matches.append(collection)
            stages = self._translate_filters(tps, collection)

            queries_per_collection[collection] = {
                '$match': and_matches,
                '$project': projections,
                'filter_stages': stages,
                'result_temp': sparql_result_template
            }

        first = True
        outer_collection = None

        # new optimized query translation
        if len(full_matches) < 1:
            return [], [], []

        query = [{'$unwind': '$@graph'}, {'$match': queries_per_collection[full_matches[0]]['$match']}]
        for stage in queries_per_collection[full_matches[0]]['filter_stages']:
            query.append(stage)
        query.append({'$project': queries_per_collection[full_matches[0]]['$project']})
        sparql_result_templates = {}
        sparql_result_templates.update(queries_per_collection[full_matches[0]]['result_temp'])
        return query, full_matches, sparql_result_templates

        # # old query translation
        # unwinds = [{'$unwind': '$@graph'},
        #            {'$unwind': '$@graph.@type'}]
        # outer_query = unwinds
        # outer_projection = {}
        # sparql_result_templates = {}
        # filter_stages = []
        # if len(full_matches) > 0:
        #     outer_collection = full_matches[0]
        #     full_matches.remove(outer_collection)
        #     query = queries_per_collection[outer_collection]
        #     outer_projection = query['$project']
        #     match = {'$match': query['$match']}
        #     outer_query.append(match)
        #     filter_stages = query['filter_stages']
        #     sparql_result_templates.update(query['result_temp'])
        #
        #     first = False
        #
        # i = 0
        # for c, query in queries_per_collection.items():
        #     if first:
        #         outer_collection = c
        #         outer_projection = query['$project']
        #         match = {'$match': query['$match']}
        #         outer_query.append(match)
        #         filter_stages = query['filter_stages']
        #         sparql_result_templates.update(query['result_temp'])
        #         first = False
        #         continue
        #     # In case of full matching collection is already set
        #     if c == outer_collection:
        #         continue
        #
        #     sparql_result_templates.update(query['result_temp'])
        #
        #     if c not in full_matches:
        #         query['$match']['$expr']['$and'].append({'$eq': ['$@graph.@id', '$$v' + str(i)]})
        #         lookup = {
        #             '$lookup': {
        #                 'from': c,
        #                 'let': {'v' + str(i): '$@graph.@id'},
        #                 'pipeline': [
        #                     {'$unwind': '$@graph'},
        #                     {'$unwind': '$@graph.@type'},
        #                     {'$match': query['$match']}
        #                 ],
        #                 'as': 'joinValues' + str(i)
        #             }
        #         }
        #         for stage in query['filter_stages']:
        #             lookup['$lookup']['pipeline'].append(stage)
        #
        #         lookup['$lookup']['pipeline'].append({'$project': query['$project']})
        #
        #         outer_query.append(lookup)
        #         outer_query.append({'$unwind': '$joinValues' + str(i)})
        #         for p, v in query['$project'].items():
        #             if p == '_id':
        #                 continue
        #             outer_projection[p] = '$joinValues' + str(i) + '.' + p
        #     else:
        #         unionwith = {"$unionWith": {
        #                     "coll": c,
        #                     "pipeline": [
        #                             {'$unwind': '$@graph'},
        #                             {'$unwind': '$@graph.@type'},
        #                             {'$match': query['$match']}
        #                         ]}}
        #         for stage in query['filter_stages']:
        #             unionwith['$unionWith']['pipeline'].append(stage)
        #
        #         unionwith['$unionWith']['pipeline'].append({'$project': query['$project']})
        #
        #         outer_query.append(unionwith)
        #
        #     i += 1
        # # print(outer_collection)
        # # print(outer_projection)
        # for stage in filter_stages:
        #     outer_query.append(stage)
        # projects = {'$project': outer_projection}
        # outer_query.append(projects)
        # return outer_query, outer_collection, sparql_result_templates
