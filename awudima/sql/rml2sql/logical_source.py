__author__ = "Kemele M. Endris"

from awudima.pyrml import LogicalSource
from awudima.sql.lang.model import SQLTable, SQLSubQuery, SQLFromExpression


class LogicalSource2SQL:
    """
    Translates RML LogicalSource mapping to an SQL FROM clause.
    Example 1:
        ```
        rml:logicalSource [
          rml:source <#PLATOON_DB>;
           rr:sqlVersion rr:SQL2008;
           rr:tableName "sometable"
        ];
        ```
        Will be translated to SQL FROM clause as:
        ```
            [FROM] sometable as logical_source_relation_1
        ```
        `self.from_expr` represents the FROM clause translated above as `SQLFromExpression` object.

    Example 2:
        ```
        rml:logicalSource [
          rml:source <#PLATOON_DB>;
           rr:sqlVersion rr:SQL2008;
           rml:query "SELECT * FROM sometable WHERE cost > 1000 "
        ];
        ```
        Will be translated to SQL FROM clause as:
        ```
            [FROM] (SELECT * FROM sometable WHERE cost > 1000 ) as logical_source_relation_1
        ```
        `self.from_expr` represents the FROM clause translated above as `SQLFromExpression` object.

    """

    _global_table_counter = 0

    @staticmethod
    def get_global_table_counter(prefix='logical_source_'):
        LogicalSource2SQL._global_table_counter += 1
        return prefix + str(LogicalSource2SQL._global_table_counter)

    def __init__(self, logical_source: LogicalSource, schema=None):
        self.logical_source = logical_source
        self.from_expr = None
        self.schema = schema
        self._process_logical_source()

    def _process_logical_source(self):
        """
        Translate LogicalSource to SQL FROM clause statement

        :return: SQLFromExpression object representing the SQL FROM clause equivalent of the RML Logical source object
        """
        import hashlib
        if self.logical_source.table_name is not None:
            table_name = SQLTable(self.logical_source.table_name, self.schema)
        elif self.logical_source.query is not None:
            self.logical_source.query = self.logical_source.query.strip()
            if '""' in self.logical_source.query[:3] and '""' in self.logical_source.query[-3:]:
                self.logical_source.query = self.logical_source.query[2:-2]

            table_name = SQLSubQuery(self.logical_source.query)
        else:
            table_name = SQLTable(str(hashlib.md5(str(self.logical_source.source.name).encode()).hexdigest()),
                                  self.schema)
        logical_relation_alias = LogicalSource2SQL.get_global_table_counter('logical_source_relation_')
        self.from_expr = SQLFromExpression(table_name, logical_relation_alias)

    @staticmethod
    def get_table_name(logical_source):
        """
        Get table name, query, or hashed pathname as string, no alias is generated to it.

        :param logical_source:

        :return: name of table or query view as relation or hashed filename of the source.
        """
        import hashlib
        if logical_source.table_name is not None:
            table_name = logical_source.table_name
        elif logical_source.query is not None:
            table_name = '(' + logical_source.query + ')'
        else:
            table_name = str(hashlib.md5(str(logical_source.source.name).encode()).hexdigest())
        return table_name

    def __str__(self):
        return str(self.from_expr)

    def __repr__(self):
        return self.__str__()


if '__main__' == __name__:
    from pyrml import RMLSource, DataSourceType

    # Data source desc
    s = RMLSource("mysqlsource",
                  ds_desc={"http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN": "dbname",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#username": "root",
                           "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#password": "mypassword"},
                  dstype=DataSourceType.MYSQL, dbmstype='MySQL')
    from pprint import pprint
    pprint(s.to_json())

    # Logical source mapping
    ls = LogicalSource(s, 'row', reference_formulation="MySQL")
    ls.table_name = "sometable"
    # ls.query = "SELECT * FROM sometable "
    pprint(ls.to_json())

    # LogicalSource to SQL FROM clause
    sqlv = LogicalSource2SQL(ls)
    pprint(sqlv.from_expr)

    # get only table/relation value without alias
    pprint(LogicalSource2SQL.get_table_name(ls))
