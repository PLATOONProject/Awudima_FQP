__author__ = "Kemele M. Endris"

from typing import Dict, List
from awudima.pyrml import TripleMap, DataSourceType


class DataSource(object):
    """ Data source descriptions

       Represents a data source in a semantic data lake. A data source is identified by its id and url.
       """
    def __init__(self,
                 name,
                 dsId,
                 url,
                 dstype,
                 params: Dict = None,
                 mapping_paths: List = None,
                 desc='',
                 acronym=None,
                 mappings: Dict = None,
                 typing_predicate='a',
                 labeling_property="http://www.w3.org/2000/01/rdf-schema#label",):
        """
        :param dsId:
        :param dstype: type of the source system: one of
                        [SPARQL_Endpoint, MySQL, Postgres, MongoDB, Neo4j, HADOOP_CSV, HADOOP_XML,
                         HADOOP_JSON, HADOOP_TSV, SPARK_CSV, SPARK_TSV, SPARK_XML, SPARK_JSON,
                         REST, CSV, TSV, JSON, XML, TXT]
        :param url: path to the dataset/file/api. Could be file://.., hdfs://.., http://.., ftp://..
        :param name: name of the datasource
        :param desc: short description of the data stored in this data source
        :param acronym: short acronym, if available
        :param params: a key-value pair of other configuration parameters
        :param mappings:
        :param typing_predicate:
        :param labeling_property:
        """
        self.name = name
        self.ID = dsId
        self.dsId = dsId
        self.url = url
        self.params = params
        if self.params is None:
            self.params = {}
        self.mappings = mappings
        if self.mappings is None:
            self.mappings = {}

        self.tripleMaps = None
        self.dstype = dstype

        self.desc = desc
        self.acronym = acronym

        self.policy = None
        if typing_predicate.strip() == 'a':
            typing_predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
        self.typing_predicate = typing_predicate
        self.labeling_property = labeling_property

        # in case of mapping endpoint, data source ID (self.dsId) should be an IRI used as value of the logical source
        self.mapping_paths = mapping_paths # path or a list of paths to mapping file/endpoint
        if self.mapping_paths is None:
            self.mapping_paths = []
        self.mappings_type = None  # RML file, RML endpoint, R2RML-file, R2RML-endpoint

    def to_str(self):
        """Produces a text representation of this data source

        :return: text representation as dsId(name)
        """

        return self.dsId

    def to_json(self):
        """Produces a JSON representation of this data source

        :return: json representation of this data source
        """

        return {
            "dsId": self.dsId,
            "name": self.name,
            "desc": self.desc,
            "url": self.url,
            "dstype": self.dstype.value if isinstance(self.dstype, DataSourceType) else self.dstype,
            "params": self.params,
            "acronym": self.acronym,
            'labeling_property': self.labeling_property,
            "mappings": {uri: m.to_json() for uri, m in self.mappings.items()},
            'mapping_paths': self.mapping_paths, # if self.mappings is not None else "",
            'mappings_type': self.mappings_type,
            "typing_predicate": self.typing_predicate,
            "policy": self.policy # if self.policy is not None else ""
        }

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.dsId == other.dsId and self.url == other.url

    def __hash__(self):
        return hash(self.dsId + '-' + self.url)

    @staticmethod
    def load_from_json(sources):
        data_sources = []
        for sr in sources:
            dstype = DataSourceType[sr['dstype'].upper()] if sr['dstype'] is not None else None
            ds = DataSource(sr['name'],
                            sr['dsId'],
                            url=sr['url'],
                            dstype=dstype,
                            desc=sr['desc'])
            if 'acronym' in sr and sr['acronym'] != '':
                ds.acronym = sr['acronym']
            if 'typing_predicate' in sr:
                ds.typing_predicate = sr['typing_predicate']
            if 'labeling_property' in sr:
                ds.labeling_property = sr['labeling_property']
            if 'params' in sr:
                ds.params = sr['params']
            if 'mapping_paths' in sr:
                ds.mapping_paths = sr['mapping_paths']
            if 'mappings_type' in sr:
                ds.mappings_type = sr['mappings_type']
            if 'policy' in sr:
                ds.policy = sr['policy']

            if 'mappings' in sr:
                ds.mappings = TripleMap.load_from_json(sr['mappings'])

            data_sources.append(ds)

        return data_sources
