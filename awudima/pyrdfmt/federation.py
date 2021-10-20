
__author__ = "Kemele M. Endris"

from awudima.pyrml import DataSourceType
from awudima.pyrdfmt.sparql_endpoint import SPARQLEndpointRDFMT
from awudima.pyrdfmt.rdfmt import RDFMT
from awudima.pyrdfmt.datasource import DataSource
from awudima.pyrdfmt.extractor import RDFMTExtractor


class Federation(object):
    """Description of data source federation

    It represents a virtual view over group of data sources in a semantic data lake.
    This class represents a set of data sources in a semantic data lake and provides a virtual view over them.
    """
    def __init__(self, fedId, name, desc):
        """
        :param fedId: str ID of the federation
        :param name: short name of the federation
        :param desc: short description of the intended domain or project the federation will serve
        """

        self.fedId = fedId
        self.name = name
        self.desc = desc
        self.datasources = set()
        self.rdfmts = set()

        self._predIdx = None
        self._mt_dict = None
        self._mt_obj = None
        self._typing_predicates = None
        self._datasources_dict = None

    @property
    def typing_predicates(self):
        if self._typing_predicates is not None:
            return self._typing_predicates

        typing_preds = []
        for d in self.datasources:
            if d.typing_predicate.strip() == 'a':
                typing_predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                typing_preds.append(typing_predicate)
            else:
                typing_preds.append(d.typing_predicate)

        self._typing_predicates = list(set(typing_preds))
        return self._typing_predicates

    @property
    def predicate_rdfmts(self):
        if self._predIdx is not None:
            return self._predIdx

        idx = {}
        for m in self.rdfmts:
            for p in m.predicates:
                idx.setdefault(p.predId, []).append(m.mtId)

        self._predIdx = idx
        return idx

    @property
    def datasources_obj(self):
        if self._datasources_dict is not None:
            return self._datasources_dict
        self._datasources_dict = {s.dsId: s for s in self.datasources}

        return self._datasources_dict

    @property
    def rdfmts_dict(self):
        if self._mt_dict is not None:
            return self._mt_dict

        self._mt_dict = {r.mtId: r.to_json() for r in self.rdfmts}
        return self._mt_dict

    @property
    def rdfmts_obj(self):
        if self._mt_obj is not None:
            return self._mt_obj

        self._mt_obj = {r.mtId: r for r in self.rdfmts}
        return self._mt_obj

    def extract_molecules(self, merge=True, collect_labels=True, collect_stats=True, save_intermediate_in='./', save_unlinked_as=None):
        """
        extract RDFMT for this federation

        :param merge: whether to merge or not - replace. default True
        :param collect_labels:
        :param collect_stats:
        :param save_intermediate_in:
        :param save_unlinked_as:
        :return:
        """
        extractor = RDFMTExtractor()
        self._predIdx = None
        self._mt_dict = None
        self._mt_obj = None
        if merge:
            self.rdfmts = set()
        rdfmts_dict = self.rdfmts_obj
        i = 0
        for ds in self.datasources:
            print(ds.url)
            print("----------------------")
            mts = extractor.get_molecules(ds, collect_labels=collect_labels, collect_stats=collect_stats)
            print('total mts:', len(mts))
            for m in mts:
                if m.mtId in rdfmts_dict:
                    rdfmts_dict[m.mtId] = rdfmts_dict[m.mtId].merge_with(m)
                else:
                    rdfmts_dict[m.mtId] = m
                    # self.rdfmts.add(m)
            fname = ds.url.replace(':', '-').replace('/', '_')

            # self.dump_this_to_json(list(rdfmts_dict.values()), ds, save_intermediate_in + '/' + fname + '-desc.json')

            i += 1
            print(i, ') extracting description for ', ds.url, 'is done!')

        self.rdfmts = set(list(rdfmts_dict.values()))
        # if save_unlinked_as is None:
        #     save_unlinked_as = self.fedId.replace(':', '-').replace('/', '_') + 'no-interlinks.json'
        # self.dump_to_json(save_unlinked_as)

        self.rdfmts = SPARQLEndpointRDFMT.get_interlinks(set(list(rdfmts_dict.values())))
        return self.rdfmts

    def dump_this_to_json(self, rdfmts, datasouce, fname):
        data = {
            "fedId": self.fedId,
            "name": self.name,
            "desc": self.desc,
            'rdfmts': [r.to_json() for r in rdfmts],
            "sources": {datasouce.dsId: datasouce.to_json()}
        }
        import json
        json.dump(data, open(fname, 'w'))

    def extract_source_molecules(self, datasource, merge=True):
        """extract RDFMT for this federation

        :param merge: whether to merge or not - replace. default True
        :return:
        """
        self._predIdx = None
        self._mt_dict = None
        self._mt_obj = None
        extractor = RDFMTExtractor()
        if merge:
            toremove = []
            for m in self.rdfmts:
                if datasource in m.datasources:
                    # if this rdfmt is not available in any other data sources, then remove it completely,
                    # else just remove the datasouce from sources list
                    if len(m.datasources) == 1:
                        toremove.append(m)
                    else:
                        m.datasources.remove(datasource)

            for m in toremove:
                self.rdfmts.remove(m)

        # self.rdfmts.update(extractor.get_molecules(datasource, collect_labels=True, collect_stats=True))
        mts = extractor.get_molecules(datasource, collect_labels=True, collect_stats=True)
        rdfmts_dict = self.rdfmts_obj
        for m in mts:
            if m.mtId in rdfmts_dict:
                rdfmts_dict[m.mtId] = rdfmts_dict[m.mtId].merge_with(m)
            else:
                rdfmts_dict[m.mtId] = m

        self.rdfmts = set(list(rdfmts_dict.values()))
        return self.rdfmts

    def to_str(self):
        """Produces a text representation of the federation

        :return: text representation as fedId(name)
        """

        return self.fedId

    def addSource(self, source):
        self._typing_predicates = None
        self.datasources.add(source)

    def addRDFMT(self, rdfmt):
        self._predIdx = None
        self._mt_dict = None
        self._mt_obj = None
        rdfmts_dict = self.rdfmts_obj
        if rdfmt.mtId in rdfmts_dict:
            rdfmts_dict[rdfmt.mtId] = rdfmts_dict[rdfmt.mtId].merge_with(rdfmt)
        else:
            self.rdfmts.add(rdfmt)

    def addRDFMTs(self, rdfmts):
        self._predIdx = None
        self._mt_dict = None
        self._mt_obj = None
        # self.rdfmts.update(rdfmts)
        rdfmts_dict = self.rdfmts_obj
        for rdfmt in rdfmts:
            if rdfmt.mtId in rdfmts_dict:
                rdfmts_dict[rdfmt.mtId] = rdfmts_dict[rdfmt.mtId].merge_with(rdfmt)
            else:
                self.rdfmts.add(rdfmt)

    def to_json(self):
        """Produces a JSON representation of the federation

        :return: json representation of the Federation
        """

        return {
            "fedId": self.fedId,
            "name": self.name,
            "desc": self.desc,
            'rdfmts': [r.to_json() for r in self.rdfmts],
            "sources": {s.dsId: s.to_json() for s in self.datasources}
        }

    def dump_to_json(self, outname):
        import json
        thisjson = self.to_json()
        with open(outname, 'w') as f:
            json.dump(thisjson, f)
        return thisjson

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __hash__(self):
        return hash(self.fedId)

    @staticmethod
    def load_from_json(path):
        if isinstance(path, dict):
            fed = path
        else:
            import json
            with open(path, 'r') as f:
                fed = json.load(f)

        if 'fedId' not in fed or 'name' not in fed or 'desc' not in fed:
            return None
        federation = Federation(fed['fedId'], fed['name'], fed['desc'])
        sourceids = {}
        if 'sources' in fed:
            sources = fed['sources']
            sources = DataSource.load_from_json(sources.values())
            for s in sources:
                sourceids[s.dsId] = s
                federation.addSource(s)
        if 'rdfmts' in fed:
            rdfmts = fed['rdfmts']
            rdfmts = RDFMT.load_from_json(rdfmts, sourceids)
            for mt in rdfmts:
                federation.rdfmts.add(mt)

        return federation

    @staticmethod
    def config(path):
        if isinstance(path, dict):
            fed = path
        else:
            import json
            with open(path, 'r') as f:
                fed = json.load(f)

        if 'fedId' not in fed or 'name' not in fed or 'desc' not in fed:
            return None
        federation = Federation(fed['fedId'], fed['name'], fed['desc'])
        sourceids = {}
        if 'sources' in fed:
            sources = fed['sources']
            sources = DataSource.load_from_json(sources.values())
            for s in sources:
                sourceids[s.dsId] = s
                federation.addSource(s)
        if 'rdfmts' in fed:
            rdfmts = fed['rdfmts']
            rdfmts = RDFMT.load_from_json(rdfmts, sourceids)
            for mt in rdfmts:
                federation.rdfmts.add(mt)

        return federation
