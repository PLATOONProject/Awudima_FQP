
__author__ = "Kemele M. Endris"

from awudima.pyrdfmt.predicate import MTPredicate


class RDFMT(object):
    """Represents an RDF molecule template (RDF-MT)

        An RDF-MT is an abstract description of semantic concepts represented in a dataset.
        It is identified by its unique mtID (IRI), representing a class/concept in an ontology and it comprises a set of
        possible properties/predicates an instance of this class can have. Instances of an RDF-MT can be available in
        one or more data sources in a federation.
        """
    def __init__(self, mtId, label, mttype, desc='',  cardinality=-1):
        """
        :param mtId: unique id of the molecule template (class/concept)
        :param label: name/label of the molecule template
        :param mttype: whether it is a typed RDF-MT or not. Typed RDF-MTs are RDF-MTs that are defined by the ontology
                    and can be extracted using via 'instance of' property (rdf:type/P31) of instances.
        """

        self.mtId = mtId
        self.label = label
        self.mttype = mttype
        self.desc = desc
        self.predicates = set()
        self.datasources = set()
        self.predicate_sources = {}
        self.instance_prefixes = []
        self.cardinality = cardinality
        self.subClassOf = []
        self.constraints = []
        self.policy = None

        # mappings
        self.mappingIds = {}
        self.predicate_RML_sources = {}

        self._mt_pred_obj = None
        self._mt_pred_dict = None

    def addPredicate(self, pred, ds, rmlsource=None):
        self._mt_pred_obj = None
        self._mt_pred_dict = None
        self.predicates.add(pred)
        self.predicate_sources.setdefault(ds.dsId, set()).add(pred)
        self.addDataSource(ds)
        if rmlsource is not None:
            self.predicate_RML_sources.setdefault(pred.predId, set()).add(rmlsource)

    @property
    def preds_as_dict(self):
        if self._mt_pred_dict is not None:
            return self._mt_pred_dict

        self._mt_pred_dict = {p.predId: p.to_json() for p in self.predicates}
        return self._mt_pred_dict

    @property
    def preds_as_dict_obj(self):
        if self._mt_pred_obj is not None:
            return self._mt_pred_obj

        self._mt_pred_obj = {p.predId: p for p in self.predicates}
        return self._mt_pred_obj

    def addDataSource(self, ds):
        self.datasources.add(ds)

    def to_str(self):
        """Produces a textual representation of the molecule template

        :return: text representation as mtId(label)
        """

        return self.mtId

    def to_json(self):
        """Produces a JSON representation of the molecule template

        :return: json representation of the molecule template
        """

        return {
            "mtId": self.mtId,
            "mttype": self.mttype,
            'label': self.label,
            'desc': self.desc,
            'cardinality': self.cardinality,
            "subClassOf": self.subClassOf,
            "predicates": [p.to_json() for p in self.predicates],
            "datasources": [d.dsId for d in self.datasources],
            "predicate_sources": {d: [s.predId for s in p] for d, p in self.predicate_sources.items()},
            "predicate_RML_sources": {p: [rs.to_json() for rs in s] for p, s in self.predicate_RML_sources.items()},
            "instance_prefixes": self.instance_prefixes,
            "constraints": [c for c in self.constraints],
            "policy": self.policy,
            "mappingIds": self.mappingIds
        }

    def merge_with(self, other):
        self._mt_pred_obj = None
        self._mt_pred_obj = None

        if self.mtId != other.mtId:
            raise Exception("Cannot merge two different RDFMTs " + self.mtId + ' and ' + other.mtId)
        merged = RDFMT(self.mtId, self.label, self.mttype, self.desc, self.cardinality)
        merged.predicate_sources = self.predicate_sources
        merged.predicate_RML_sources = self.predicate_RML_sources
        merged.mappingIds.update(self.mappingIds)
        merged.mappingIds.update(other.mappingIds)

        merged.instance_prefixes = list(set(self.instance_prefixes + other.instance_prefixes))

        if self.label is None or len(self.label) == 0:
            merged.label = other.label
        if self.desc is None or len(self.desc) == 0:
            merged.desc = other.desc
        if self.cardinality == -1:
            merged.cardinality = other.cardinality
        else:
            try:
                merged.cardinality = int(merged.cardinality) + int(other.cardinality)
            except:
                pass

        merged.subClassOf.extend(other.subClassOf)
        merged.subClassOf = list(set(merged.subClassOf))

        otherpreds = other.preds_as_dict_obj
        mergedpreds = []
        difs = other.predicates.difference(self.predicates)
        for p in self.predicates:
            if p.predId in otherpreds:
                mergedpreds.append(p.merge_with(otherpreds[p.predId]))
            else:
                mergedpreds.append(p)

        for p in difs:
            mergedpreds.append(p)
        for did, p in other.predicate_sources.items():
            merged.predicate_sources.setdefault(did, set()).update(p)
        for pid, rms in other.predicate_RML_sources.items():
            merged.predicate_RML_sources.setdefault(pid, set()).update(rms)

        merged.predicates = set(mergedpreds)
        merged.datasources = self.datasources | other.datasources

        # TODO: merge constaints and access policies (restriced first approach)

        return merged

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.mtId == other.mtId

    def __hash__(self):
        return hash(self.mtId)

    @staticmethod
    def load_from_json(rdfmts, sources):
        mts = []
        for mt in rdfmts:
            if 'mtId' not in mt:
                continue

            sourceIds = {}
            lbl = mt['label'] if 'label' in mt else None
            mttype = mt['mttype'] if 'mttype' in mt else 'typed'
            desc = mt['desc'] if 'desc' in mt else ''

            rdfmt = RDFMT(mt['mtId'], lbl, mttype, desc)
            if 'cardinality' in mt:
                if '^^' in str(mt['cardinality']):
                    rdfmt.cardinality = mt['cardinality'][:mt['cardinality'].find('^^')]
                else:
                    rdfmt.cardinality = mt['cardinality']
            if 'mappingIds' in mt:
                rdfmt.mappingIds = mt['mappingIds']
            else:
                rdfmt.mappingIds = {}

            if 'policy' in mt:
                rdfmt.policy = mt['policy']
            if 'constraints' in mt:
                rdfmt.constraints = mt['constraints']
            if 'subClassOf' in mt:
                rdfmt.subClassOf = mt['subClassOf']
            if 'instance_prefixes' in mt:
                rdfmt.instance_prefixes = mt['instance_prefixes']
            if 'predicate_sources' in mt:
                predicate_sourcesdict = mt['predicate_sources']
            else:
                predicate_sourcesdict = {}

            rev_predicate_sourcesdict = {}
            for s in predicate_sourcesdict:
                for p in predicate_sourcesdict[s]:
                    rev_predicate_sourcesdict.setdefault(p, []).append(s)

            if 'predicate_RML_sources' in mt:
                predicate_RML_sourcesdict = mt['predicate_RML_sources']
                rdfmt.predicate_RML_sources = {}
                from awudima.pyrml import RMLSource, DataSourceType
                for p, rs in predicate_RML_sourcesdict.items():
                    for s in rs:
                        st = DataSourceType[s['dstype']] if s['dstype'] is not None else None
                        rdfmt.predicate_RML_sources.setdefault(p, set()).add(RMLSource(s['name'], st, s['dbmstype'], s['ds_desc']))
            else:
                rdfmt.predicate_RML_sources = {}

            if 'datasources' in mt:
                datasources = mt['datasources']
                datasources = {s: sources[s] for s in datasources if s in sources}
                for dsid, s in datasources.items():
                    sourceIds[dsid] = s
                    rdfmt.addDataSource(s)

            if 'predicates' in mt:
                predicates = mt['predicates']
                predicates = MTPredicate.load_from_json(predicates)
                for pred in predicates:

                    if pred.predId in rev_predicate_sourcesdict:
                        for s in rev_predicate_sourcesdict[pred.predId]:
                            rdfmt.addPredicate(pred, sourceIds[s])
                    else:
                        for s in rdfmt.datasources:
                            rdfmt.addPredicate(pred, s)

            mts.append(rdfmt)
        return mts
