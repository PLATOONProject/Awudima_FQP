
__author__ = "Kemele M. Endris"

from enum import Enum
from typing import Dict, List

from awudima.pysparql import SPARQLEndpointWrapper
from awudima.pyrml import TripleMap, DataSourceType

from awudima.pyrdfmt.rdfmt import RDFMT
from awudima.pyrdfmt.predicate import MTPredicate

metas = ['http://www.w3.org/ns/sparql-service-description',
         'http://www.openlinksw.com/schemas/virtrdf#',
         'http://www.w3.org/2000/01/rdf-schema#',
         'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
         'http://www.w3.org/2002/07/owl#',
         'http://purl.org/dc/terms/Dataset',
         'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType',
         'nodeID://']


class SPARQLEndpointRDFMT:

    def __init__(self):
        pass

    def get_sparql_endpoint_molecules(self,
                                      datasource,
                                      collect_labels=False,
                                      collect_stats=False,
                                      limit=-1,
                                      out_queue=None):
        """
        :param datasource:
        :param collect_labels:
        :param collect_stats:
        :param limit:
        :param out_queue:
        :return:
        """
        endpoint = datasource.url
        typing_pred = datasource.typing_predicate
        labeling_prop = datasource.labeling_property

        rdfmts = []
        concepts = self.get_concepts(endpoint, collect_labels=collect_labels, collect_stats=collect_stats,
                                     labeling_prop=labeling_prop, typing_pred=typing_pred,
                                     limit=limit, out_queue=out_queue)
        for c in concepts:
            t = c['t']
            t = t.replace('\\', "")
            label = t
            desc = ''
            if collect_labels:
                label = c['label']
                desc = c['desc']
            card = -1
            if 'card' in c:
                card = c['card']
                if '^^' in card:
                    card = card[:card.index('^^')]

            rdfmt = RDFMT(t, label, 'typed', desc=desc, cardinality=card)
            if 'subClassOf' in c:
                rdfmt.subClassOf = [s.replace('\\', "") for s in c['subClassOf']]

            preds = self.get_predicates(endpoint, t, collect_labels=collect_labels, collect_stats=collect_stats,
                                        labeling_prop=labeling_prop, limit=limit, out_queue=out_queue)

            for p in preds:
                label = p['p']
                if isinstance(label, str):
                    p['p'] = p['p'].replace('\\', "")
                    label = label.replace('\\', "")
                else:
                    # print(label)
                    continue
                desc = ''
                if collect_labels:
                    label = p['label']
                    desc = p['desc']
                card = -1
                if 'card' in p:
                    card = p['card']
                    if '^^' in card:
                        card = card[:card.index('^^')]
                pred = MTPredicate(p['p'].replace('\\', ""), label, desc=desc, cardinality=card)

                ranges = self.get_predicate_ranges(endpoint, t, p['p'])
                pred.addRanges(ranges)
                rdfmt.addPredicate(pred, datasource)

            rdfmt.addDataSource(datasource)
            insts = self.get_subj_prefixes(rdfmt.mtId, endpoint, typing_pred)
            rdfmt.instance_prefixes.append(insts)
            rdfmt.instance_prefixes = list(set(rdfmt.instance_prefixes))
            rdfmts.append(rdfmt)

        return rdfmts

    def get_concepts(self, endpoint, collect_labels=False, collect_stats=False,
                     labeling_prop="http://www.w3.org/2000/01/rdf-schema#label",
                     typing_pred='a', limit=-1, out_queue=None):
        """Entry point for extracting RDF-MTs of an endpoint.

            Extracts list of rdf:Class concepts from the endpoint

        :param endpoint:
        :param collect_labels: boolean value setting wheather to collect labels or not. default: False
        :param labeling_prop: if {collect_labels} is set `True`, then this labeling property will be used.
                        default: http://www.w3.org/2000/01/rdf-schema#label
        :param limit:
        :param typing_pred: typing predicate used in the endpoint.
                Can be any predicate uri, such as a or <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>.
                 Should be in full uri format (not prefixed as rdf:type, wdt:P31, except 'a' which is the default)
                default: rdf:type or 'a'.
        :param out_queue
        :param collect_stats
        :return:
        """
        query = "SELECT DISTINCT ?t WHERE{ ?s " + typing_pred + " ?t } "

        # if limit is not set, then set limit to 50, graceful request
        if limit == -1:
            limit = 500

        reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit)

        # exclude some metadata classes
        reslist = [r for r in reslist if True not in [m in str(r['t']) for m in metas]]
        if collect_labels:
            reslist = self.get_labels(endpoint, reslist, 't', labeling_prop, 500)
            reslist = self.get_labels(endpoint, reslist, 't', 'http://www.w3.org/2004/02/skos/core#prefLabel', 500)
            reslist = self.get_altlabels(endpoint, reslist, 't', 'http://www.w3.org/2004/02/skos/core#altLabel', 500)
        if collect_stats:
            reslist = self.get_cardinality(endpoint, reslist, 't')

        reslist = self.get_super_classes(endpoint, reslist, 't')

        return reslist

    def get_predicates(self, endpoint, rdfmt_id, collect_labels=False, collect_stats=False,
                       labeling_prop="http://www.w3.org/2000/01/rdf-schema#label",
                       limit=20, out_queue=None):
        """
         Get list of predicates of a class rdfmt_id

        :param endpoint: endpoint
        :param rdfmt_id: RDF class Concept extracted from an endpoint
        :param collect_labels: boolean value setting wheather to collect labels or not. default: False
        :param labeling_prop: if {collect_labels} is set `True`, then this labeling property will be used.
                        default: http://www.w3.org/2000/01/rdf-schema#label
        :param collect_stats:
        :param limit:
        :param out_queue:
        :return:
        """

        query = " SELECT DISTINCT ?p WHERE{ ?s a <" + rdfmt_id + ">. ?s ?p ?pt. } "

        if limit < 1:
            limit = 15
        reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit)
        if status == -1:
            # fallback strategy - get predicates from randomly selected instances of {rdfmt_id}
            print(rdfmt_id, 'properties are not all extracted properly. Falling back to randomly selected instances...')
            rand_inst_res = self._get_preds_of_sample_instances(endpoint, rdfmt_id)
            existingpreds = [r['p'] for r in reslist]
            for r in rand_inst_res:
                if r not in existingpreds:
                    reslist.append({'p': r})

        # collect labels if requested
        if collect_labels:
            reslist = self.get_labels(endpoint, reslist, 'p', labeling_prop, 500)
            reslist = self.get_labels(endpoint, reslist, 'p', 'http://www.w3.org/2004/02/skos/core#prefLabel', 500)
            reslist = self.get_altlabels(endpoint, reslist, 'p', 'http://www.w3.org/2004/02/skos/core#altLabel', 500)
        if collect_stats:
            reslist = self.get_cardinality(endpoint, reslist, 'p')

        return reslist

    def get_predicate_ranges(self, endpoint, rdfmt_id, pred_id, limit=100):
        """get value ranges/rdfs ranges of the given predicate {pred_id}

        Ranges of a predicate {pred_id} can be obtained in two ways:
            1. by using rdfs:range property, if defined by the RDFS, and
            2. by checking the rdf:type/wdt:P31 of the object values associated to the given {rdfmt_id} instance and
               its predicate {pred_id}

        :param endpoint: url
        :param rdfmt_id: rdfmt
        :param pred_id: predicate
        :param limit: int, default: 100
        :return: list of ranges
        """

        ranges = self._get_rdfs_ranges(endpoint, pred_id)
        ranges.extend(self._find_instance_range(endpoint, rdfmt_id, pred_id))

        return ranges

    def _get_rdfs_ranges(self, endpoint, pred_id, limit=-1):

        RDFS_RANGES = " SELECT DISTINCT ?range  WHERE{ <" + pred_id + "> <http://www.w3.org/2000/01/rdf-schema#range> ?range. }"

        if limit == -1:
            limit = 50

        reslist, status = SPARQLEndpointRDFMT.get_results_iter(RDFS_RANGES, endpoint, limit)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if m in r['range']:
                    skip = True
                    break
            if not skip:
                ranges.append(r['range'])

        return ranges

    def _find_instance_range(self, endpoint, rdfmt_id, pred_id, limit=-1):
        """extract ranges of a predicate {pred_id} associated to RDF-MT {rdfmt_id}

        :param endpoint:
        :param rdfmt_id:
        :param pred_id:
        :param limit:
        :return:
        """
        INSTANCE_RANGES = " SELECT DISTINCT ?r WHERE{ ?s a <" + rdfmt_id + ">. ?s <" + pred_id + "> ?pt.  ?pt a ?r  } "
        INSTANCE_RANGES_DType = " SELECT DISTINCT datatype(?pt) as ?r WHERE{ ?s a <" + rdfmt_id + ">. ?s <" + pred_id + "> ?pt. } "

        if limit == -1:
            limit = 50

        reslist, status = SPARQLEndpointRDFMT.get_results_iter(INSTANCE_RANGES, endpoint, limit)
        reslist2, status2 = SPARQLEndpointRDFMT.get_results_iter(INSTANCE_RANGES_DType, endpoint, limit)
        reslist2 = [r for r in reslist2 if len(r) > 0]
        if len(reslist2) > 0:
            reslist.extend(reslist2)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if 'r' in r and m in r['r']:
                    skip = True
                    break
            if not skip:
                if 'r' in r:
                    ranges.append(r['r'])

        return ranges

    @staticmethod
    def get_results_iter(query, endpoint, limit, max_rows=-1, out_queue=None):
        offset = 0
        reslist = []
        status = 0

        while True:
            query_copy = query + " LIMIT " + str(limit) + (" OFFSET " + str(offset) if offset > 0 else '')
            res, card = SPARQLEndpointWrapper.contact_sparql_endpoint(query_copy, endpoint)

            # in case source fails because of the data/row limit, try again up to limit = 1
            if card == -2:
                limit = limit // 2
                if limit < 1:
                    status = -1
                    break
                continue

            # if results are returned from the endpoint, append them to the results list
            if card > 0:
                reslist.extend(res)

                # if output queue is given, then put each non-metadata classes to the queue
                if out_queue is not None:
                    for r in res:
                        if True not in [m in str(r['t']) for m in metas]:
                            out_queue.put(r)

            # if number of rows returned are less than the requested limit, then we are done
            if card < limit or (max_rows > 0 and len(reslist) >= max_rows):
                break
            offset += limit

        return reslist, status

    def _get_preds_of_sample_instances(self, endpoint, rdfmt_id, limit=50):

        """get a union of predicates from the first 100 subjects returned

        :param endpoint: endpoint
        :param rdfmt_id: rdf class concept of and endpoint
        :param limit:
        :return:
        """

        query = " SELECT DISTINCT ?s WHERE{ ?s a <" + rdfmt_id + ">. } "

        if limit < 1:
            limit = 50

        reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit, max_rows=100)
        results = []
        batches = []
        for i in range(len(reslist)):
            inst = reslist[i]
            if len(batches) == 10 or i + 1 == len(batches):
                inst_res = self._get_preds_of_instances(endpoint, batches)
                inst_res = [r['p'] for r in inst_res]
                results.extend(inst_res)
                # reslist = list(set(reslist))
                batches = []
            else:
                batches.append(inst['s'])

        return list(set(results))

    def _get_preds_of_instances(self, endpoint, insts, limit=100):
        """get union of predicates from the given set of instances, {insts}

        :param endpoint: url
        :param insts: list of instances
        :param limit: limit, default= 100
        :return:
        """

        unions = ["{ <" + inst + "> ?p ?pt } " for inst in insts]
        query = " SELECT DISTINCT ?p WHERE{ " + " UNION ".join(unions) + " } "
        reslist = []
        if limit < 1:
            limit = 100
        offset = 0

        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = SPARQLEndpointWrapper.contact_sparql_endpoint(query_copy, endpoint)

            # in case source fails because of the data/row limit, try again up to limit = 1
            if card == -2:
                limit = limit // 2
                if limit < 1:
                    break
                continue

            # if results are returned from the endpoint, append them to the results list
            if card > 0:
                reslist.extend(res)

            # if number of rows returned are less than the requested limit, then we are done
            if card < limit:
                break

            offset += limit

        return reslist

    def get_labels(self, endpoint, ids, key, labeling_prop, limit):
        """Collect labels for the given uris in a dictionary {ids}

        :param endpoint: sparql endpoint
        :param ids: list of dict values
        :param key: key to access the rdfmt_id or pred_id
        :param labeling_prop:
        :param limit:

        :return: updated list {ids} with additional element 'label'
        """
        result = []
        batches = []
        for i in range(0, len(ids), 10):
            batches = ids[i: i+10] if i+10 < len(ids) else ids[i:]

            ggp = ["{ <" + batches[j][key].replace('\\', '')+ ">  <" + labeling_prop + "> ?l" + str(
                j) + " . filter (regex(lang(?l" + str(j) + "), 'en','i' ) || regex(lang(?l" + str(j) + "), '','i' ))}"
                   for j in range(len(batches)) if isinstance(batches[j][key], str)]

            query = "SELECT DISTINCT * WHERE{" + " UNION ".join(ggp) + "} "
            reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit)

            # set the default label, i.e., same as its id
            for b in batches:
                if 'label' not in b:
                    b['label'] = b[key]

            for r in reslist:
                for j in range(len(batches)):
                    if 'l' + str(j) in r and len(r['l' + str(j)]) > 0 and batches[j]['label'] == batches[j][key]:
                        batches[j]['label'] = r['l' + str(j)]
            result.extend(batches)
            # reset batches list to empty

        return result

    def get_altlabels(self, endpoint, ids, key, labeling_prop, limit):
        """Collect labels for the given uris in a dictionary {ids}

        :param endpoint: sparql endpoint
        :param ids: list of dict values
        :param key: key to access the rdfmt_id or pred_id
        :param labeling_prop:
        :param limit:

        :return: updated list {ids} with additional element 'label'
        """
        result = []
        batches = []
        for i in range(0, len(ids), 10):
            batches = ids[i: i+10] if i+10 < len(ids) else ids[i:]

            ggp = ["{ <" + batches[j][key].replace('\\', '')+ ">  <" + labeling_prop + "> ?l" + str(
                j) + " . filter (regex(lang(?l" + str(j) + "), 'en','i' ) || regex(lang(?l" + str(j) + "), '','i' ))}"
                   for j in range(len(batches)) if isinstance(batches[j][key], str)]

            query = "SELECT DISTINCT * WHERE{" + " UNION ".join(ggp) + "} "
            reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit)

            # set the default label, i.e., same as its id
            for b in batches:
                b['desc'] = b[key]

            for r in reslist:
                for j in range(len(batches)):
                    if 'l' + str(j) in r and len(r['l' + str(j)]) > 0:
                        batches[j]['desc'] = r['l' + str(j)]
            result.extend(batches)
            # reset batches list to empty

        return result

    def get_super_classes(self, endpoint, ids, key, limit=15):
        """
        Collect all superclasses of the given RDF-MT {rdfmt_id}

        :param endpoint:
        :param ids:
        :param key:
        :param limit:
        :return:
        """
        results = []
        for t in ids:
            rdfmt_id = t[key].replace('\\', '')
            # uses path query to get all superclasses, since subClassOf property is transitive
            query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
                    " SELECT DISTINCT ?sc WHERE { <" + rdfmt_id + "> rdfs:subClassOf* ?sc }"
            # if limit is not set, then set limit to 50, graceful request
            if limit == -1:
                limit = 15

            reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, limit)

            # exclude some metadata classes
            reslist = [r['sc'] for r in reslist if True not in [m in str(r['sc']) for m in metas] and r != rdfmt_id]
            t['subClassOf'] = reslist
            results.append(t)

        return results

    def get_cardinality(self, endpoint, ids, key):
        """collect cardinality of the given RDF-MT {rdfmt_id}

        :param endpoint:
        :param ids:
        :param key:
        :return:
        """
        results = []
        for t in ids:
            rdfmt_id = t[key]

            query = " SELECT COUNT(DISTINCT ?s) as ?card WHERE {?s a <" + rdfmt_id + "> }"

            reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, 10)

            # set cardinality as unknown (-1)
            card = -1

            if reslist is not None and len(reslist) > 0:
                card = reslist[0]['card']
                if '^^' in str(card):
                    card = card[:card.index('^^')]

            t['card'] = card
            results.append(t)

        return results

    def get_subj_prefixes(self, rdfmt_id, endpoint, typing_pred='a'):
        """
        Collect prefixes of subject resources for each RDFMT
        Note: assumes each source have a unique resource identification prefix for each molecule

        :param rdfmt_id:
        :param endpoint:
        :param typing_pred:
        :return:
        """
        prefix = ''
        query = "SELECT ?s  WHERE {?s " + typing_pred + " <" + rdfmt_id + ">}"
        reslist, status = SPARQLEndpointRDFMT.get_results_iter(query, endpoint, 10, max_rows=10)
        inst = list(set([t['s'][:t['s'].rfind('/')] if '/' in t['s'] else t['s'] for t in reslist]))

        if len(inst) > 0:
            first = inst[0]
            if len(first) >= 7+first[7:].find('/')+1:
                prefix = first[:7+first[7:].find('/')+1]
            else:
                prefix = first

            for i in range(7, min([len(ins) for ins in inst])):
                if len(set([c[i] for c in inst])) == 1:
                    prefix += inst[0][i]
                else:
                    prefix = first
                    break
        return prefix

    @staticmethod
    def get_interlinks(rdfmts):

        for m1 in rdfmts:
            m1_pred_dict_objs = m1.preds_as_dict_obj
            for m2 in rdfmts:
                if m1 == m2:
                    continue
                if len(m1.datasources) == 1 == m2.datasources and m1.datasources == m2.datasources:
                    continue

                # << ==
                m2_prefix = m2.instance_prefixes
                filters = [" regex(?o, \"" + pr + '", "i") ' for pr in m2_prefix]
                filters_txt = " || ".join(filters)

                for ds in m1.datasources:
                    if ds.dstype != DataSourceType.SPARQL_ENDPOINT:
                        continue
                    query = "SELECT DISTINCT ?p WHERE { ?s " + ds.typing_predicate + " " + m1.mtId + \
                            " . ?s ?p ?o FILTER (" + filters_txt + ") } "
                    result, status = SPARQLEndpointRDFMT.get_results_iter(query, ds.url, 10)
                    for p in result:
                        pred = p['p']
                        m1pred = m1_pred_dict_objs[pred]
                        m1.predicates.remove(m1pred)
                        m1.predicate_sources[ds.dsId].remove(m1pred)

                        m1pred.addRanges([m2.mtId])
                        m1.predicates.add(m1pred)
                        m1.predicate_sources[ds.dsId].add(m1pred)

                # == >>
                m2_pred_dict_objs = m2.preds_as_dict_obj
                m1_prefix = m1.instance_prefixes
                filters = [" regex(?o, \"" + pr + '", "i") ' for pr in m1_prefix]
                filters_txt = " || ".join(filters)

                for ds in m2.datasources:
                    if ds.dstype != DataSourceType.SPARQL_ENDPOINT:
                        continue
                    query = "SELECT DISTINCT ?p WHERE { ?s " + ds.typing_predicate + " " + m2.mtId + \
                            " . ?s ?p ?o FILTER (" + filters_txt + ") } "
                    result, status = SPARQLEndpointRDFMT.get_results_iter(query, ds.url, 10)
                    for p in result:
                        pred = p['p']
                        m2pred = m2_pred_dict_objs[pred]
                        m2.predicates.remove(m2pred)
                        m2.predicate_sources[ds.dsId].remove(m2pred)

                        m2pred.addRanges([m1.mtId])
                        m2.predicates.add(m2pred)
                        m2.predicate_sources[ds.dsId].add(m2pred)

        return rdfmts
