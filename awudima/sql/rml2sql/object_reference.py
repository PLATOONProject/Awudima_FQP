
__author__ = "Kemele M. Endris"

from awudima.sql.lang.model import SQLSelectQuery
from awudima.sql.lang.model import SQLSelectExpression, SQLColumn, SQLCondition, SQLAndCondition
from awudima.sql.rml2sql.logical_source import LogicalSource2SQL
from awudima.sql.rml2sql.term_map import TermMap2SQL

from awudima.pyrml import ObjectReferenceMap, TripleMap
from awudima.pysparql import Argument


class ObjectReferenceMap2SQL:
    def __init__(self,
                 ref_obj_map: ObjectReferenceMap,
                 parent_map: TripleMap,
                 object_rdf_term: Argument,
                 comparision_opr='=',
                 table_alias=None,
                 schema=None):

        self.ref_obj_map = ref_obj_map
        self.parent_map = parent_map
        self.object_rdf_term = object_rdf_term
        self.comparison_opr = comparision_opr

        self.projection = None
        self.filter_conditions = None
        self.referenced_table = None
        self.join_conditions = []

        self.schema = schema
        self.table_alias = table_alias

        self.ref_sql = None

        self._process_term_map()

    def _process_term_map(self):
        """
        If the rdf_term is constant, then extract filter conditions but there will not be any projections
        Otherwise, project the term's mapping column/item and make sure the values are bound (not null or empty)
        Called when this object is init or either of its property (term_map/rdf_term) are updated
        using setter methods defined below
        :return:
        """

        subject_query = TermMap2SQL(self.parent_map.subject_map.subject, self.object_rdf_term, self.comparison_opr,
                                    self.table_alias, self.schema)

        self.projection = subject_query.projection
        self.filter_conditions = subject_query.filter_conditions

        parent_columns = []
        if len(self.ref_obj_map.parent_columns) > 0:
            parent_columns = [SQLColumn(parent_column, self.table_alias, self.schema)
                              for parent_column in self.ref_obj_map.parent_columns]
            sql_conditions = [SQLCondition(col, ' IS NOT ', 'NULL')
                              for col in parent_columns]
            if self.filter_conditions is not None:
                self.filter_conditions.conditions.extend(sql_conditions)
            else:
                self.filter_conditions = SQLAndCondition(sql_conditions)

        self.join_conditions = self.ref_obj_map.join_conditions

        sql = SQLSelectQuery(True)
        if self.projection is not None:
            if parent_columns:
                select_exprs = [SQLSelectExpression(term, term.name) for term in parent_columns] + [self.projection]
            else:
                select_exprs = [self.projection]
            sql.select_exprs = select_exprs

        sql.where_exprs = []
        if self.filter_conditions is not None:
            sql.where_exprs.append(self.filter_conditions)

        self.referenced_table = LogicalSource2SQL(self.parent_map.logical_source, self.schema)
        sql.from_exprs = [self.referenced_table.from_expr]

        self.ref_sql = sql


if __name__ == '__main__':
    from awudima.pyrml import TermMap, PredicateMap, ObjectMap, TermType, SubjectMap, TripleMapType, RMLSource, LogicalSource, TripleMap, DataSourceType

    # Data source desc
    s = RMLSource("mysqlsource",
                  ds_desc={"http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN": "dbname",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#username": "root",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#password": "mypassword"},
                  dstype=DataSourceType.MYSQL, dbmstype='MySQL')
    # Logical source mapping
    ls = LogicalSource(s, 'row', reference_formulation="MySQL")
    ls.table_name = "sometable"
    # ls.query = "SELECT * FROM sometable "
    # pprint(ls.to_json())
    tmcty = TermMap('http://hello.us/City/{name}', TripleMapType.TEMPLATE, TermType.IRI)
    tmcry = TermMap('http://hello.us/Country/{name}', TripleMapType.TEMPLATE, TermType.IRI)
    cmap = SubjectMap("_:lksdjflsd", tmcty, rdf_types=['http://ontology.com/City', 'http://schema.org/City'])
    pmap = SubjectMap("_:lajdlfsdf", tmcry, rdf_types=['http://ontology.com/Country'])

    pred = TermMap('http://hello.us/name', TripleMapType.CONSTANT, TermType.IRI)
    prm = PredicateMap("_:kjdhfkja", pred)

    obj = TermMap('name', TripleMapType.REFERENCE, TermType.Literal)
    objm = ObjectMap("_:lksdajfer", obj, language='en')
    pom = [(prm, objm)]

    parent_triplemap = TripleMap('http://triplemaps.map/CountryMapping', ls, pmap, pom)

    obref = ObjectReferenceMap(parent_triplemap.uri, [], termtype=TripleMapType.TRIPLEMAP)
    objrefmap = ObjectMap('_:ljdalsdkf', obref)

    cpred = TermMap('http://hello.us/country', TripleMapType.CONSTANT, TermType.IRI)
    cprm = PredicateMap("_:kjdhfkja", cpred)
    opom = [(cprm, objrefmap)]

    triplemap = TripleMap('http://triplemaps.map/CityMapping', ls, cmap, opom)
    v = Argument("?nvar", False)

    # pprint(parent_triplemap.to_json())
    # pprint(triplemap.to_json())

    refq = ObjectReferenceMap2SQL(obref, parent_triplemap, v)

    print('Projection:', refq.projection)
    print('WHERE condition:', refq.filter_conditions)
    print('Reference table:', refq.referenced_table)

    print('join conditions:', refq.join_conditions)
    print("Parent map Query:\n")
    print(refq.ref_sql)

    v = Argument("<http://hello.us/Country/Germany>", True)

    # pprint(parent_triplemap.to_json())
    # pprint(triplemap.to_json())
    print("---------------------")
    refq = ObjectReferenceMap2SQL(obref, parent_triplemap, v)
    print('Projection:', refq.projection)
    print('WHERE condition:', refq.filter_conditions)
    print('Reference table:', refq.referenced_table)

    print('join conditions:', refq.join_conditions)
    print("Parent map Query:\n")
    print(refq.ref_sql)