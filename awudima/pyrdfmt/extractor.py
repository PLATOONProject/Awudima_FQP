__author__ = "Kemele M. Endris"

from awudima.pyrml import DataSourceType
from awudima.pyrdfmt.sparql_endpoint import SPARQLEndpointRDFMT
from awudima.pyrdfmt.mongodb_store import MongoLDFlattenRDFMT
from awudima.pyrdfmt.rdfmt import RDFMT
from awudima.pyrdfmt.predicate import MTPredicate


class RDFMTExtractor:
    """ Extracts RDF-MTs from a sparql endpoint, or other sources

    ATM this class only implements the sparql endpoint sources
    """

    def __init__(self, sink_type='memory', path_to_sink='', params=None):
        """

        :param sink_type: sink to save/dump the molecule templates. default: memory
        :param path_to_sink: path to the sink. Either path to a json file or uri to sparql endpoint/mongodb collection.
        :param params: other parameters
        """

        self.sink_type = sink_type
        self.path_to_sink = path_to_sink
        self.params = params

    def get_molecules(self, datasource, collect_labels=False, collect_stats=False,
                      limit=-1, out_queue=None):

        if datasource.dstype == DataSourceType.SPARQL_ENDPOINT:
            endpointrdfmt = SPARQLEndpointRDFMT()
            return endpointrdfmt.get_sparql_endpoint_molecules(datasource, collect_labels, collect_stats, limit, out_queue)
        elif datasource.dstype == DataSourceType.MONGODB_LD_FLAT:
            mongordfmt = MongoLDFlattenRDFMT()
            return mongordfmt.get_mongo_json_ld_flattened_molecules(datasource, collect_labels, collect_stats, limit, out_queue)
        elif datasource.dstype != DataSourceType.SPARQL_ENDPOINT:
            if datasource.mapping_paths is None or len(datasource.mapping_paths) == 0:
                if datasource.dstype == DataSourceType.NEO4J:
                    return []
                elif datasource.dstype == DataSourceType.MONGODB:
                    return []
            else:
                if datasource.mappings_type == 'RML file':
                    from awudima.pyrml import RML
                    mapping = RML(datasource.url + '_mapping', datasource.mapping_paths, datasource.mappings_type)
                    mapping.load()
                    datasource.mappings.update(mapping.triple_maps)
                    rdfmts = self.get_rml_molecules(datasource, mapping, collect_labels, collect_stats)
                    return rdfmts
            return []

    def get_rml_molecules(self, datasource, mapping, collect_labels=False, collect_stats=False):
        """

        :param datasource:
        :param mapping:
        :param collect_labels:
        :param collect_stats:
        :return:
        """
        rdfmts = {}
        range_prefixes = {}
        from awudima.pyrml import TripleMapType, TermMap
        for t, tm in mapping.triple_maps.items():

            rdftypes = tm.subject_map.rdf_types
            mttype = 'typed'
            if len(rdftypes) == 0:
                rdftypes = [t]
                mttype = 'triplemapid'
            label = ''
            instprefixes = []
            if collect_labels:
                if tm.subject_map.subject.resource_type == TripleMapType.TEMPLATE:
                    label = tm.subject_map.subject.value
                    cols = tm.subject_map.subject.value.split('{')
                    if len(cols) > 1:
                        labels = [lbl[:lbl.rfind('}')] for lbl in cols[1:]]
                        label = "_".join(labels)

                    instprefixes.append(tm.subject_map.subject.value[:tm.subject_map.subject.value.find('{')])
                else:
                    label = tm.subject_map.subject.value
            preds = []
            pred_obj_prefix = {}
            for p, o in tm.predicate_obj_map:
                ranges = []
                pred = p.predicate.value

                if p.predicate.resource_type == TripleMapType.CONSTANT:
                    pred = p.predicate.value
                elif p.predicate.resource_type == TripleMapType.REFERENCE or p.predicate.resource_type == TripleMapType.TEMPLATE:
                    # need to access all the row of the referenced column/item/element
                    pass
                if o.data_type is not None:
                    ranges.append(o.data_type)
                if not isinstance(o.objectt, TermMap):
                    # if triplemapid type then use the object.value
                    if len(mapping.triple_maps[o.objectt.value].subject_map.rdf_types) == 0:
                        ranges.append(o.objectt.value)
                    else:
                        ranges.extend(mapping.triple_maps[o.objectt.value].subject_map.rdf_types)
                if isinstance(o.objectt, TermMap):
                    if pred == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' and o.objectt.resource_type == TripleMapType.CONSTANT:
                        tm.subject_map.rdf_types.append(o.objectt.value)
                        rdftypes.append(o.objectt.value)
                    if o.objectt.resource_type == TripleMapType.TEMPLATE:

                        if '{' not in o.objectt.value:
                            pred_obj_prefix[pred] = o.objectt.value[:o.objectt.value.rfind('/')+1]
                        else:
                            pred_obj_prefix[pred] = o.objectt.value[:o.objectt.value.find('{')]

                p = MTPredicate(pred, pred)
                p.addRanges(ranges)
                preds.append(p)

            # add typing predicate of the data source to the list of predicates
            p = MTPredicate(datasource.typing_predicate[1:-1], datasource.typing_predicate[1:-1])
            preds.append(p)

            for mlt in rdftypes:
                rdfmt = RDFMT(mlt, label, mttype, cardinality=-1)

                rdfmt.addDataSource(datasource)
                rdfmt.instance_prefixes = list(set(instprefixes))
                for pred in preds:
                    rdfmt.addPredicate(pred, datasource, tm.logical_source.source)

                rdfmt.mappingIds.setdefault(datasource.dsId, {}).setdefault(tm.uri, []).extend(rdftypes)
                # rdfmt.mappings = [tm.filename]

                range_prefixes[mlt] = pred_obj_prefix

                if mlt in rdfmts:
                    rdfmts[mlt] = rdfmts[mlt].merge_with(rdfmt)
                else:
                    rdfmts[mlt] = rdfmt

        for i, s in mapping.sources.items():
            datasource.params.update(s.ds_desc)

        # check links based on prefixes of subjectmap and objectmaps
        #for m in rdfmts:
        #    for m2, prx in range_prefixes.items():
        #        if len(set(rdfmts[m].instance_prefixes).intersection(set(prx.values()))) > 0:
        #            for p, r in prx.items():
        #                if len(set(rdfmts[m].instance_prefixes).intersection([r])) > 0:
        #                    pred = rdfmts[m2].preds_as_dict_obj[p]

        #                    rdfmts[m2].predicates.remove(pred)
        #                    rdfmts[m2].predicate_sources[datasource.dsId].remove(pred)
        #                    pred.addRanges([m])

        #                    rdfmts[m2].predicates.add(pred)
        #                    rdfmts[m2].predicate_sources[datasource.dsId].add(pred)

        return list(rdfmts.values())
