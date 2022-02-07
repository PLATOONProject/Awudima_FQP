__author__ = "Kemele M. Endris"

from pymongo import MongoClient
from requests.utils import requote_uri
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MongoDBWrapper:

    def __init__(self, datasource, config):
        self.datasource = datasource
        self.config = config
        self.url = datasource.url
        self.params = datasource.params
        self.username = None
        self.password = None
        self.database_name = None
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

    def init_connection(self):
        try:
            if self.username is None:
                mongo_client = MongoClient(self.url)
            else:
                mongo_client = MongoClient(self.url,
                                           username=self.username,
                                           password=self.password)

        except Exception as ex:
            print("Exception while connecting to MongoDB", ex)
            logger.error("Exception while connecting to MongoDB" + str(ex))
            return None

        return mongo_client

    def executeQuery(self, mongo_ql, queue, sparql_result_template, limit=-1, offset=0):
        if self.database_name is None:
            queue.put('EOF')
            return
        db_name = mongo_ql['db']
        coll_name = mongo_ql['collection']
        pipeline = mongo_ql['pipeline']

        mongo_client = self.init_connection()
        # print(mongo_client.server_info())
        # mongo_client = MongoClient(host=self.host, port=self.port)
        if mongo_client is None:
            queue.put('EOF')
            return

        try:
            db = mongo_client.get_database(self.database_name)
            for col_name in mongo_ql['collection']:
                source = db_name + '<|>' + col_name
                collection = db.get_collection(col_name)
                result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
                cardinality = self.process_result(result, queue, sparql_result_template, source)
        except IOError as ie:
            print("IO ERROR:", ie)
            logger.error("IOError while running query: " + str(ie))
        except Exception as e:
            print("Exception: ", e)
            logger.error("Exception while running query: " + str(e))
        finally:
            queue.put('EOF')
            return

#         collection = db.get_collection(coll_name)
#
#         card = 0
#         if limit == -1:
#             limit = 10000
#         if offset == -1:
#             offset = 0
#
#         logger.info(mongo_ql)
#         try:
#             # rs = time()
#             while True:
#                 query_copy = [p for p in pipeline]
#                 query_copy.extend([{"$skip": offset}, {"$limit": limit}])
#                 #result = collection.aggregate(query_copy, useCursor=True, batchSize=1000, allowDiskUse=True)
#                 collection = db.get_collection("scada_10min_converter")
#                 result = collection.aggregate([{'$unwind': '$@graph'},
#                                                {'$match': {'$expr': {'$and': [{'$ne': ['$@graph.evaluatedSimpleValue', 'null']},
#                                                                               {'$ne': ['$@graph.evaluatedSimpleValue', '']}]},
#                                                            '@graph.@type': {'$in': ['seas:TemperatureEvaluation']}}},
#                                                {'$project': {'_id': 0, 'label': '$@graph.evaluatedSimpleValue', 'windfarm': '$@graph.@id'}}], useCursor=True, batchSize=1000, allowDiskUse=True)
#                 result = collection.aggregate([
#     {'$unwind': '$@graph'},
#                     {'$match': {"@graph.inXSDDateTime": {'$gte': "2020-11-17T22:20:00Z", '$lte': "2022-11-29T22:20:00Z"}}},
#                     {'$match': {"@graph.@type": {'$in': ['time:Instant']}}},
#     {'$project': {'_id': 0, 'instance': '$@graph.@id', 'time': '$@graph.inXSDDateTime', 'type': '$@graph.@type'}}
#   ], useCursor=True, batchSize=1000, allowDiskUse=True)
#
#
#                 ## alternative
#                 result = collection.aggregate([
#                     {'$unwind': '$@graph'},
#                     {'$match':
#                         {
#                             '$and': [
#                                 {"@graph.inXSDDateTime": {'$gte': "2020-11-17T22:20:00Z",
#                                                           '$lte': "2022-11-29T22:20:00Z"}},
#                                 {"@graph.@type": {'$in': ['time:Instant']}}
#                             ]
#                         }},
#                     {'$project': {'_id': 0, 'instance': '$@graph.@id', 'time': '$@graph.inXSDDateTime',
#                                   'type': '$@graph.@type'}}
#                 ], useCursor=True, batchSize=1000, allowDiskUse=True)
#
#
# #                result = collection.find({'$expr': {'$and': [{'$ne': ['$@graph.seas:evaluatedSimpleValue', 'null']},
# #                                                             {'$ne': ['$@graph.seas:evaluatedSimpleValue', '']}]},
# #                                          '@graph.@type': {'$in': ['seas:TemperatureEvaluation']}}, {'_id': 0,
# #               'label': '$@graph.seas:evaluatedSimpleValue',
# #               'windfarm': '$@graph.@id'})
#                 cardinality = self.process_result(result, queue, sparql_result_template)
#                 card += cardinality
#                 # if (time()-rs) > 20:
#                 #     print(card, 'results found -..')
#                 if cardinality < limit:
#                     break
#                 offset = offset + limit
#             logger.info("Running query: " + str(mongo_ql) + " is DONE with cardinality: " + str(card))
#         except IOError as ie:
#             print("IO ERROR:", ie)
#             logger.error("IOError while running query: " + str(ie))
#             pass
#         except Exception as e:
#             print("EXception: ", e)
#             logger.error("Exception while running query: " + str(e))
#             pass
#
#         queue.put("EOF")
#         # print(mongo_ql)
#         # print('DONE')

    def process_result(self, results, queue, sparql_result_template: dict, source: str):
        if results is None:
            print("empty results")
            return 0
        c = 0

        for doc in results:
            # print("res:", doc, "\n")
            c += 1
            row = {}
            res = sparql_result_template.copy()
            skip = False
            for k, v in doc.items():
                row[k] = v

                if row[k] == 'null':
                    skip = True
                    break
                if k in sparql_result_template:
                    value = str(row[k])
                    res[k]['source'] = [source]
                    try:
                        if res[k]['type'] == 'uri':
                            if len(value) > 2 and '_:' in value[:3]:
                                res[k]['type'] = 'bnode'
                            value = requote_uri(value)
                        if res[k]['type'] == 'bnode':
                            value = '_:' + requote_uri(value)
                        res[k]['value'] = value
                    except Exception as e:
                        print(value)
                        print("Ex: ", e)
                        skip = True
                        break

            if not skip:
                # print('wrapper:', res)
                queue.put(res)
        return c

    @staticmethod
    def contact_mongo_client(pipeline, datasource, collection_name, username=None, password=None):
        client = MongoClient(datasource.url, username=username, password=password)
        db = client.get_database(datasource.name)
        collection = db.get_collection(collection_name)
        result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
        if result is None:
            return {}
        result = [doc for doc in result]
        return result, len(result)


if __name__ == '__main__':
    from pprint import pprint
    client = MongoClient(host='0.0.0.0')
    print(client.server_info())
    print(client.database_names())
    db = client.get_database('pilot3a_engie')
    pprint(db.collection_names())
    collection = db.get_collection('occupancy')

    pipeline = [{'$unwind': '$@graph'},
                 {'$unwind': '$@graph.@type'},
                 {'$match': {'$expr': {'$and': [{'$ne': ['$@graph.containsZone', 'null']},
                                                {'$ne': ['$@graph.containsZone', '']},
                                                {'$ne': ['$@graph.label', 'null']},
                                                {'$ne': ['$@graph.label', '']}]},
                             '@graph.@type': {'$in': ['bot:Building']}}},
                 {'$project': {'_id': 0,
                               'building': '$@graph.@id',
                               'label': '$@graph.label',
                               'zone': '$@graph.containsZone'}},
                 {'$skip': 0},
                 {'$limit': 10000}]
    result = collection.aggregate(pipeline, useCursor=True, batchSize=1000, allowDiskUse=True)
    if result is None:
        print('empty results')
    for doc in result:
        pprint(doc)