__author__ = "Kemele M. Endris"

from awudima.pyrdfmt.rdfmt import RDFMT
from awudima.pyrdfmt.predicate import MTPredicate
from awudima.pyrml import RMLSource, DataSourceType

from pymongo import MongoClient


RDFMT_PIPELINE = [
        {'$unwind': '$@graph'},
        {'$unwind': '$@graph.@type'},
        {'$group': {'_id': '$@graph.@type', "objs": {'$addToSet': '$$ROOT'}}},
        {'$unwind': '$objs'},
        {'$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    {'rdftype': '$_id'},
                    {'context': '$objs.@context'},
                    {'graph': "$objs.@graph"}
                ]

            }
        }
        },
        {'$project': {'rdftype': 1, 'arrayofkeyvalue': {'$objectToArray': '$graph'}, 'prefixes': '$context'}},
        {'$unwind': '$arrayofkeyvalue'},
        {'$replaceRoot': {'newRoot': {
            '$mergeObjects': [{'rdftype': '$rdftype'}, {'prefixes': '$prefixes'}, '$arrayofkeyvalue']}}},
        {'$lookup': {
            'from': 'bms-controller',
            'let': {'eval_id': '$v', 'prop': '$k'},
            'pipeline': [
                {'$unwind': '$@graph'},
                {'$unwind': '$@graph.@type'},
                {'$match': {'$expr': {'$and': [{'$eq': ['$@graph.@id', '$$eval_id']}, {'$ne': ['$$prop', '@id']},
                                               {'$ne': ['$$prop', '@type']}]}}},
                {'$project': {'_id': 0, 'range': '$@graph.@type'}}
            ],
            'as': 'ranges'
        }
        },
        {'$match': {'$expr': {'$and': [{'$ne': ['$k', '@id']}, {'$ne': ['$k', '@type']}]}}},
        {'$group': {
            "_id": {'rdfmt': '$rdftype', 'prefixes': '$prefixes'},
            'predicates': {'$addToSet': {'predicate': '$k', 'ranges': '$ranges.range'}},

        }},
        {'$unwind': {'path': '$predicates', 'preserveNullAndEmptyArrays': True}},
        {'$replaceRoot': {
            'newRoot': {'$mergeObjects': [{'rdfmt': '$_id.rdfmt'}, {'context': '$_id.prefixes'}, '$predicates']}}},
        {'$unwind': {'path': '$ranges', 'preserveNullAndEmptyArrays': True}}
    ]


class MongoLDFlattenRDFMT:

    def __init__(self):
        self.params = {}
        self.username = None
        self.password = None
        self.url = None
        self.database_name = None

    def init_connection(self):
        try:
            if self.username is None:
                mongo_client = MongoClient(host=self.host, port=self.port)
            else:
                mongo_client = MongoClient(host=self.host,
                                           port=self.port,
                                           username=self.username,
                                           password=self.password)

        except Exception as ex:
            print("Exception while connecting to MongoDB", ex)
            return None

        return mongo_client

    def extract_params(self, datasource):
        self.params = datasource.params
        self.url = datasource.url
        if datasource.params is not None and len(datasource.params) > 0:
            if isinstance(datasource.params, dict):
                self.username = datasource.params['username'] if 'username' in datasource.params else None
                self.password = datasource.params['password'] if 'password' in datasource.params else None
                dbkey = '<http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN>'
                self.database_name = datasource.params[dbkey] if dbkey in datasource.params else None
            else:
                maps = datasource.params.split(';')
                for m in maps:
                    params = m.split(':')
                    if len(params) > 0:
                        if 'username' == params[0]:
                            self.username = params[1]
                        if 'password' == params[0]:
                            self.password = params[1]
                        if 'database' == params[0]:
                            self.database_name = params[1]
        if ':' in self.url:
            self.host, self.port = self.url.split(':')
            try:
                self.port = int(self.port)
            except:
                self.port = 27017
        else:
            self.host = self.url
            self.port = 27017

    def contact_mongo_client(self, pipeline, collection_name, client):
        db = client.get_database(self.database_name)
        collection = db.get_collection(collection_name)
        result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
        if result is None:
            return {}
        result = [doc for doc in result]
        return result, len(result)

    def get_mongo_json_ld_flattened_molecules(self,
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
        self.extract_params(datasource)

        client = self.init_connection()

        pipeline = RDFMT_PIPELINE
        db = client.get_database(self.database_name)
        collections = db.list_collection_names()
        rdfmts = []
        rdfmts_by_collection = {}
        sources_by_collection = {}
        for c in collections:
            pipeline[8]['$lookup']['from'] = c
            res, card = self.contact_mongo_client(pipeline, c, client)
            for doc in res:
                prefixes = doc['context']

                if 'ranges' in doc:
                    range = doc['ranges']
                else:
                    range = None

                rdfmt = doc['rdfmt']
                if ':' in rdfmt and rdfmt.find(':') == rdfmt.rfind(':'):
                    prefix = rdfmt[:rdfmt.find(':')]
                    if prefix in prefixes:
                        rdfmt = rdfmt.replace(prefix + ':', prefixes[prefix])
                predicate = doc['predicate']
                predId = doc['predicate']
                res_type = prefixes[predId]['@type'] if predId in prefixes and '@type' in prefixes[predId] else None
                if predicate in prefixes:
                    predicate = prefixes[doc['predicate']]['@id']

                    if range is None and '@type' in prefixes[predId] and prefixes[predId]['@type'] != '@id':
                        range = prefixes[predId]['@type']
                elif ':' in predicate and predicate.find(':') == predicate.rfind(':'): # this should not happen
                    prefix = predicate[:predicate.find(':')]
                    if prefix in prefixes:
                        if isinstance(prefixes[prefix], str):
                            predId = predicate
                            predicate = predicate.replace(prefix + ':', prefixes[prefix])
                        else:
                            pred = predicate.replace(':', '')

                            if pred in prefixes:
                                res_type = prefixes[pred]['@type'] if pred in prefixes and '@type' in prefixes[pred] else None
                                predId = predicate
                                predicate = prefixes[pred]['@id']

                                if range is None and '@type' in prefixes[pred] and prefixes[pred]['@type'] != '@id':
                                    range = prefixes[pred]['@type']

                sources_by_collection.setdefault(c, {"predicates": {}, 'prefixes': {}})
                sources_by_collection[c]['predicates'].setdefault(predicate, {})
                sources_by_collection[c]['predicates'][predicate]['@id'] = predId
                if res_type is not None:
                    sources_by_collection[c]['predicates'][predicate]['@type'] = res_type
                for p, v in prefixes.items():
                    if isinstance(v, str):
                        sources_by_collection[c]['prefixes'][p] = v

                if range is not None:
                    if ':' in range and range.find(':') == range.rfind(':'):
                        prefix = range[:range.find(':')]
                        if prefix in prefixes:
                            range = range.replace(prefix + ':', prefixes[prefix])

                    rdfmts_by_collection.setdefault(c, {}).setdefault(rdfmt, {}).setdefault(predicate, []).append(range)
                    rdfmts_by_collection[c][rdfmt][predicate] = list(set(rdfmts_by_collection[c][rdfmt][predicate]))

                else:
                    rdfmts_by_collection.setdefault(c, {}).setdefault(rdfmt, {})[predicate] = []

        datasource.params.update(sources_by_collection)

        for c, mts in rdfmts_by_collection.items():
            rmlsource = RMLSource(datasource.name + '<|>' + c,
                                  DataSourceType.MONGODB_LD_FLAT,
                                  dbmstype='MongoDB',
                                  ds_desc={
                                      "jdbcdriver": "",
                                      "collection_name": c,
                                      'database_name': datasource.name,
                                      "dbms_version": "4.0.7"
                                  })
            for mt, preds in mts.items():
                rdfmt = RDFMT(mt, mt, 'typed')
                for pred, ranges in preds.items():
                    mtpred = MTPredicate(pred, pred)
                    mtpred.addRanges(ranges)
                    rdfmt.addPredicate(mtpred, datasource, rmlsource)
                rdfmt.addDataSource(datasource)
                rdfmts.append(rdfmt)

        return rdfmts
