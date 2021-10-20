__author__ = 'Kemele M. Endris'

from multiprocessing import Queue

from awudima.mediator.decomposer.QueryDecomposer import AwudimaDecomposer
from awudima.mediator.planner.QueryPlanner import AwudimaPlanner
from awudima.pyrdfmt import Federation
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class AwudimaFQP(object):

    def __init__(self, federation: Federation):
        self.federation = federation

    def execute(self, sparql_query: str, keep_in_memory=False, pushdownssqjoins=False):
        """

        Execute a federated sparql query over a semantic data lake

        :param sparql_query:
        :param pushdownssqjoins:
        :return: yields SPARQL JSON Results
        """
        try:
            mc = AwudimaDecomposer(sparql_query, self.federation, pushdownssqjoins=pushdownssqjoins)
            decompositions = mc.decompose()
        except Exception as e:
            print("Exception while decomposing the given query: ", sparql_query)
            print(e)
            logger.info("Exception while decomposing the given query: " + str(sparql_query))
            logger.error(e)
            return None

        try:
            mwp = AwudimaPlanner(sparql_query, decompositions, self.federation, pushdownjoins=pushdownssqjoins)
            plan = mwp.create_physical_plan()
        except Exception as e:
            print("Exception while creating physical plan for the given query: ", sparql_query)
            print(e)
            logger.info("Exception while creating physical plan for  the given query: " + str(sparql_query))
            logger.error(e)
            return None

        out = Queue()

        try:
            plan.execute(out)
        except Exception as e:
            print("Exception while executing the plan for the given query: ", sparql_query)
            print(e)
            logger.info("Exception while executing the plan for  the given query: " + str(sparql_query))
            logger.error(e)
            return None

        return self.ResultSet(sparql_query, out, decompositions, plan, keep_in_memory)

    def get_sources_selected(self, sparql_query: str, pushdownssqjoins=False):
        try:
            mc = AwudimaDecomposer(sparql_query, self.federation, pushdownssqjoins=pushdownssqjoins)
            sources = mc.select_sources()
            from pprint import pformat
            return pformat(sources)
        except Exception as e:
            print("Exception while selecting sources to the given query: ", sparql_query)
            print(e)
            logger.info("Exception while selecting sources to the given query: " + str(sparql_query))
            logger.error(e)
            return None

    def get_decompose_query(self, sparql_query: str, pushdownssqjoins=False):
        try:
            mc = AwudimaDecomposer(sparql_query, self.federation, pushdownssqjoins=pushdownssqjoins)
            decomposed_query = mc.decompose()
            return decomposed_query
        except Exception as e:
            print("Exception while getting decomposed query: ", sparql_query)
            print(e)
            logger.info("Exception while  getting decomposed query: " + str(sparql_query))
            logger.error(e)
            return None

    def get_physical_plan(self, sparql_query: str, pushdownssqjoins=False):
        try:
            mc = AwudimaDecomposer(sparql_query, self.federation, pushdownssqjoins=pushdownssqjoins)
            decompositions = mc.decompose()
        except Exception as e:
            print("Exception while decomposing the given query: ", sparql_query)
            print(e)
            logger.info("Exception while decomposing the given query: " + str(sparql_query))
            logger.error(e)
            return None

        try:
            mwp = AwudimaPlanner(sparql_query, decompositions, self.federation, pushdownjoins=pushdownssqjoins)
            plan = mwp.create_physical_plan()
            return plan
        except Exception as e:
            print("Exception while creating physical plan for the given query: ", sparql_query)
            print(e)
            logger.info("Exception while creating physical plan for  the given query: " + str(sparql_query))
            logger.error(e)
            return None

    class ResultSet(object):
        def __init__(self, sparql_query, queue: Queue, decomposition, plan, keep_in_memory=False):

            self.queue = queue
            self.sparql_query = sparql_query
            self.decomposition = decomposition
            self.plan = plan
            self.keep_in_memory = keep_in_memory

            self.__retrieved_results = -1
            self.__retrieval_status = "Not Started"
            self.__result_buffer = {
                "head": {
                    "vars": []
                },
                "results": {
                    "bindings": []
                },
                'message': '',
                'query': str(self.sparql_query)
            }

        @property
        def status(self):
            return self.__retrieval_status

        @property
        def retrieved_results(self):
            """
            total number of results found in ResultSet queue. If the results are not collected yet,
            this property will force the retrieval of results from the queue and store in memory.
            :return:
            """

            if self.__retrieval_status == "Not Started":
                res = list(self.get())
                self.__result_buffer['results']['bindings'] = res

                self.__retrieved_results = len(res)

                if len(self.decomposition.args) == 0:
                    from awudima.pysparql import parser as queryParser
                    query = queryParser.parse(self.sparql_query)
                    self.__result_buffer['head']['vars'] = query.getVars()
                else:
                    self.__result_buffer['head']['vars'] = [var[1:] for var in self.decomposition.args]
            return self.__retrieved_results

        @property
        def results(self):
            message = "All results are retrieved from resultset!"
            if self.__retrieval_status == "Not Started":
                res = list(self.get())
                self.__result_buffer['results']['bindings'] = res
                self.__retrieved_results = len(res)

                if len(self.decomposition.args) == 0:
                    from awudima.pysparql import parser as queryParser
                    query = queryParser.parse(self.sparql_query)
                    self.__result_buffer['head']['vars'] = query.getVars()
                else:
                    self.__result_buffer['head']['vars'] = [var[1:] for var in self.decomposition.args]

            if self.__retrieval_status == 'Interrupted':
                message = "Data retrieval has been interrupted because of some Exception!"
            elif self.__retrieval_status == "Started":
                message = "Data retrieval has not been finished yet. The results in this ResultSet are partial results!"

            if not self.keep_in_memory:
                message = "This ResultSet is not configured to keep results in memory."

            self.__result_buffer['message'] = message

            return self.__result_buffer

        def get(self):
            if self.__retrieval_status in ['Finished', 'Interrupted']:
                if self.keep_in_memory:
                    if 'results' in self.__result_buffer:
                        for r in self.__result_buffer['results']['bindings']:
                            yield r
                else:
                    yield
            else:
                try:
                    self.__retrieval_status = 'Started'
                    r = self.queue.get()

                    i = 0
                    self.__retrieved_results = 0

                    if len(self.decomposition.args) == 0:
                        from awudima.pysparql import parser as queryParser
                        query = queryParser.parse(self.sparql_query)
                        self.__result_buffer['head']['vars'] = query.getVars()
                    else:
                        self.__result_buffer['head']['vars'] = [var[1:] for var in self.decomposition.args]
                    while r != 'EOF':
                        self.__retrieved_results = i + 1
                        if self.keep_in_memory:
                            self.__result_buffer['results']['bindings'].append(r)

                        yield r
                        r = self.queue.get()
                        i += 1

                    self.__retrieval_status = "Finished"

                except Exception as ex:
                    print("Exception while retrieving results for the given query: ", self.sparql_query)
                    print(ex)
                    logger.info("Exception while retrieving results for  the given query: " + str(self.sparql_query))
                    logger.error(ex)
                    self.__retrieval_status = "Interrupted"
                    yield