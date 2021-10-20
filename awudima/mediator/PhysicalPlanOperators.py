
__author__ = 'Kemele M. Endris'

from multiprocessing import Process, Queue
from awudima.pyrml import DataSourceType

from awudima.wrappers import RDFStore
from awudima.wrappers import MySQLWrapper
from awudima.wrappers import MongoDBWrapper

# from wrappers.mongodb.sparql2mongo import SPARQL2Mongo
# from wrappers.neo4j.sparql2cypher import SPARQL2Cypher
# from wrappers.spark.sparql2spark import SPARKWrapper
# from wrappers.spark.sparql2sparksql import SPARKXMLWrapper

# from ontario.wrappers.drill.sparql2drill import DrillWrapper

from typing import Set


class NodeOperator(object):
    '''
        Represents a plan to be executed by the engine.

        It is composed by a left node, a right node, and an operator node.
        The left and right nodes can be leaves to contact sources, or subtrees.
        The operator node is a physical operator, provided by the engine.

        The execute() method evaluates the plan.
        It creates a process for every node of the plan.
        The left node is always evaluated.
        If the right node is an independent operator or a subtree, it is evaluated.
        '''
    def __init__(self, operator, vars, config, left=None, right=None, consts: Set = None, query=None):
        self.operator = operator
        self.vars = vars
        self.left = left
        self.left_ds_type = DataSourceType.SPARQL_ENDPOINT
        if isinstance(left, LeafOperator):
            self.left_ds_type = left.datasource.dstype
        elif isinstance(left, NodeOperator):
            self.left_ds_type = left.dstype

        self.consts = consts
        if self.consts is None:
            self.consts = set()

        self.right = right
        self.right_ds_type = DataSourceType.SPARQL_ENDPOINT
        if isinstance(right, LeafOperator):
            self.right_ds_type = right.datasource.dstype
        elif isinstance(right, NodeOperator):
            self.right_ds_type = right.dstype

        self.cardinality = None
        self.joinCardinality = []
        self.query = query
        self.config = config
        self.dstype = None
        if self.left_ds_type is not None and self.right_ds_type is not None:
            if self.left_ds_type == self.right_ds_type:
                self.dstype = self.left_ds_type

    def __repr__(self):
        return self.aux(" ")

    def instantiate(self, d):
        l = None
        r = None
        if self.left:
            l = self.left.instantiate(d)
        if self.right:
            r = self.right.instantiate(d)
        newvars = self.vars - set(d.keys())

        return NodeOperator(self.operator.instantiate(d), newvars, l, r)

    def instantiateFilter(self, d, filter_str):
        l = None
        r = None
        if self.left:
            l = self.left.instantiateFilter(d, filter_str)
        if self.right:
            r = self.right.instantiateFilter(d, filter_str)
        newvars = self.vars - set(d)
        return NodeOperator(self.operator.instantiateFilter(d, filter_str), newvars, l, r)

    def allTriplesLowSelectivity(self):
        a = True
        if self.left:
            a = self.left.allTriplesLowSelectivity()
        if self.right:
            a = a and self.right.allTriplesLowSelectivity()
        return a

    def places(self):
        p = 0
        if self.left:
            p = self.left.places()
        if self.right:
            p = p + self.right.places()
        return p

    def constantNumber(self):
        c = 0
        if self.left:
            c = self.left.constantNumber()
        if self.right:
            c = c + self.right.constantNumber()
        return c

    def constantPercentage(self):
        return self.constantNumber()/self.places()

    def getCardinality(self):

        if self.cardinality is None:
            self.cardinality = self.operator.getCardinality(self.left, self.right)
        return self.cardinality

    def getJoinCardinality(self, vars):
        c = None
        for (v, c2) in self.joinCardinality:
            if v == vars:
                c = c2
                break
        if c is None:
            c = self.operator.getJoinCardinality(self.left, self.right, vars)
            self.joinCardinality.append((vars, c))
        return c

    def aux(self, n):
        s = n + str(self.operator) + "\n" + n + str(self.vars) + "\n"
        if self.left:
            s = s + self.left.aux(n+"  ")

        if self.right:
            s = s + self.right.aux(n + "  ")
        return s

    def exec(self, outputqueue, processqueue=Queue()):
        # Evaluates the execution plan.
        if self.left: #and this.right: # This line was modified by mac in order to evaluate unary operators
            qleft = Queue()
            qright = Queue()

            # The left node is always evaluated.
            # Create process for left node
            # print "self.right: ", self.right
            #print "self.left: ", self.left

            p1 = Process(target=self.left.execute, args=(qleft, processqueue,))
            p1.start()
            processqueue.put(p1.pid)
            from operators.nonblocking.NestedHashJoinFilter import NestedHashJoinFilter
            from operators.nonblocking.NestedHashOptionalFilter import NestedHashOptionalFilter
            if isinstance(self.operator, NestedHashJoinFilter) or isinstance(self.operator, NestedHashOptionalFilter):
                p3 = Process(target=self.operator.execute, args=(qleft, self.right, outputqueue, processqueue, ))
                p3.start()
                processqueue.put(p3.pid)
                p1.join()
                p3.join()
                return

            # Check the right node to determine if evaluate it or not.
            if self.right and (isinstance(self.right, LeafOperator) or isinstance(self.right, NodeOperator)):
                p2 = Process(target=self.right.execute, args=(qright, processqueue, ))
                p2.start()
                processqueue.put(p2.pid)
            else:
                qright = None
                # qright.put("EOF")
                p2 = None

            # Create a process for the operator node.
            p = Process(target=self.operator.execute, args=(qleft, qright, outputqueue, processqueue, ))
            #print "left and right "
            # Execute the plan
            p.start()
            processqueue.put(p.pid)

            p1.join()
            if p2 is not None:
                p2.join()
            p.join()

    def execute(self, outq, processq=Queue()):
        if self.left:
            qleft = Queue()
            qright = Queue()
            # print('Left:', self.left.__class__.__name__)
            # if isinstance(self.left, NodeOperator):
            #     print('left_op:', self.left.operator)
            p1 = Process(target=self.left.execute, args=(qleft, ))
            p1.start()

            if self.right:
                # print('right:', self.left.__class__.__name__)
                p2 = Process(target=self.right.execute, args=(qright,))
                p2.start()

            # print('op:', self.operator.__class__.__name__)
            p3 = Process(target=self.operator.execute, args=(qleft, qright, outq,))
            p3.start()


class LeafOperator(object):
    """
    Implements an operator that can be resolved independently.

    It receives as input the url of the server to be contacted,
    the filename that contains the query, the header size of the
    of the messages.

    The execute() method reads tuples from the input queue and
    response message and the buffer size (length of the string)
    place them in the output queue.
    """
    def __init__(self, query, tree, config):
        (e, sq, vs, cvs, res_temps) = tree.getInfoIO(query, config)
        self.datasource = tree.service.datasource
        self.triples = tree.service.triples
        self.rdfmts = tree.service.rdfmts
        self.stars = tree.service.stars
        self.server = e
        self.query = query
        self.result_template = res_temps
        self.tree = tree
        self.query_str = sq
        self.vars = vs
        self.consts = cvs
        self.buffersize = 1638400
        self.config = config
        self.cardinality = None
        self.joinCardinality = []

    def __repr__(self):
        return str(self.tree)

    def instantiate(self, d):
        new_tree = self.tree.instantiate(d)
        return LeafOperator(self.query, new_tree, self.config)

    def instantiateFilter(self, vars_instantiated, filter_str):
        new_tree = self.tree.instantiateFilter(vars_instantiated, filter_str)
        return LeafOperator(self.query, new_tree, self.config)

    def getCardinality(self):
        if self.cardinality is None:
            self.cardinality = self.askCount(self.query, self.tree, set(), self.datasource)
        return self.cardinality

    def askCount(self, query, tree, vars, contact):
        (server, query) = tree.getCount(query, vars, None)
        q = Queue()
        contact(server, query, q)

        res = q.get()
        # print res
        v = -1
        if res == "EOF":
            return 20000
        for k in res:
            v = res[k]
        q.get()
        return int(v)

    def getJoinCardinality(self, vars):
        c = None
        for (v, c2) in self.joinCardinality:
            if v == vars:
                c = c2
                break
        if c is None:
            if len(vars) == 0:
                c = self.getCardinality()
            else:
                c = self.askCount(self.query, self.tree, vars, self.datasource)
            self.joinCardinality.append((vars, c))
        return c

    def allTriplesLowSelectivity(self):
        return self.tree.service.allTriplesLowSelectivity()

    def places(self):
        return self.tree.places()

    def constantNumber(self):

        return self.tree.constantNumber()

    def constantPercentage(self):
        return self.constantNumber()/self.places()

    def aux(self, n):
        return self.tree.aux(n)

    def execute(self, outputqueue, processqueue=Queue()):

        if (self.datasource.dstype == DataSourceType.MYSQL or self.datasource.dstype == DataSourceType.SPARQL_ENDPOINT) and self.tree.service.limit == -1:
            self.tree.service.limit = 10000

        # Evaluate the independent operator.
        # q = Queue()
        pf = Process(target=self.get_wrapper_fun(self.datasource).executeQuery, args=(self.query_str, outputqueue, self.result_template, self.tree.service.limit, -1,))
        pf.start()
        # processqueue.put(p.pid)
        # r = q.get(True)
        # while r != 'EOF':
        #     print('LeafPlan', r)
        #     outputqueue.put(r)
        #     r = q.get(True)
        # outputqueue.put('EOF')
        # p.join()

    def get_wrapper_fun(self, datasource):
        if datasource.dstype == DataSourceType.MONGODB_LD_FLAT:
            return MongoDBWrapper(datasource, self.config)
        # if datasource.dstype == DataSourceType.MONGODB:
        #     return SPARQL2Mongo(datasource, self.config, self.rdfmts, self.stars)
        # elif datasource.dstype == DataSourceType.LOCAL_TSV or datasource.dstype == DataSourceType.LOCAL_CSV \
        #     or datasource.dstype == DataSourceType.LOCAL_JSON or \
        #         datasource.dstype == DataSourceType.HADOOP_TSV or datasource.dstype == DataSourceType.HADOOP_CSV \
        #         or datasource.dstype == DataSourceType.HADOOP_JSON:
        #     # DrillWrapper(datasource, self.config, self.rdfmts, self.star)
        #     return SPARKWrapper(datasource, self.config, self.rdfmts, self.stars)
        # elif datasource.dstype == DataSourceType.NEO4J:
        #     return SPARQL2Cypher(datasource, self.config, self.rdfmts, self.stars)
        if datasource.dstype == DataSourceType.SPARQL_ENDPOINT:
            return RDFStore(datasource, self.config)
        # elif datasource.dstype == DataSourceType.SPARK_XML or datasource.dstype == DataSourceType.LOCAL_XML:
        #     return SPARKXMLWrapper(datasource, self.config, self.rdfmts, self.stars)
        # elif datasource.dstype == DataSourceType.SPARK_TSV or datasource.dstype == DataSourceType.SPARK_CSV\
        #         or datasource.dstype == DataSourceType.SPARK_JSON:
        #     return SPARKWrapper(datasource, self.config, self.rdfmts, self.stars)

        if datasource.dstype == DataSourceType.MYSQL:
            return MySQLWrapper(datasource, self.config)
        else:
            raise ValueError("No such wrapper for the given data source type: " + str(datasource.dsId) +
                             "_" + str(datasource.dstype))
