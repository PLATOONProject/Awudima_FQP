__author__ = "Kemele M. Endris"

from multiprocessing import Queue
from awudima.pysparql import SPARQLEndpointWrapper
from requests.utils import requote_uri
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RDFStore(object):
    def __init__(self, datasource, config):
        self.datasource = datasource
        self.cofig = config

    def executeQuery(self, sparql_query, queue, sparql_result_template, limit=-1, offset=0):

        card = 0
        if limit == -1:
            limit = 10000
        if offset == -1:
            offset = 0

        logger.info(sparql_query)

        try:
            # rs = time()
            while True:
                query_copy = str(sparql_query) + "\n LIMIT " + str(limit) + " OFFSET " + str(offset)
                print("_______________________")
                print(query_copy)
                res, cardinality = SPARQLEndpointWrapper.contact_sparql_endpoint(query_copy, self.datasource.url)
                self.process_result(res, sparql_result_template, queue)
                card += cardinality
                # if (time()-rs) > 20:
                #     print(card, 'results found -..')
                if cardinality < limit:
                    break
                offset = offset + limit
            logger.info("Running query: " + str(sparql_query) + " Non UNION DONE" + str(card))
        except IOError as ie:
            print("IO ERROR:", ie)
            logger.error("IOError while running query: " + str(ie))
            pass
        except Exception as e:
            print("EXception: ", e)
            logger.error("Exception while running query: " + str(e))
            pass

        queue.put("EOF")

    def process_result(self, res, sparql_result_template, queue):
        for r in res:
            r['source'] = self.datasource.name
            queue.put(r)
