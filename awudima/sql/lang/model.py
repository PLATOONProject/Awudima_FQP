
__author__ = "Kemele M. Endris"


from typing import List, Dict

##################################################################
# #################  SQL Query  ##################################
##################################################################


class SQLSelectQuery:
    def __init__(self, distinct=True, select_exprs=None, from_exprs=None, where_exprs=None):
        self.distinct = distinct
        self.select_exprs = select_exprs
        self.from_exprs = from_exprs
        self.where_exprs = where_exprs
        if self.select_exprs is None:
            self.select_exprs = []
        if self.from_exprs is None:
            self.from_exprs = []
        if self.where_exprs is None:
            self.where_exprs = []

        self.where_exprs = list(set(self.where_exprs))
        self.select_exprs = list(set(self.select_exprs))
        self.from_exprs = list(set(self.from_exprs))

    def __str__(self):
        self.where_exprs = list(set(self.where_exprs))
        self.select_exprs = list(set(self.select_exprs))
        self.from_exprs = list(set(self.from_exprs))
        sql = "SELECT" + ("" if not self.distinct else " DISTINCT ")
        if len(self.select_exprs) == 0:
            sql += ' * '
        else:
            sql += ", ".join([str(se) for se in self.select_exprs])
        sql += "\nFROM "
        sql += (", \n".join([str(fe) for fe in self.from_exprs]))
        if len(self.where_exprs) > 0:
            sql += "\nWHERE "
            sql += (" AND \n".join([str(wc) for wc in self.where_exprs]))
        return sql

    def sort_projections(self):
        select_expr = dict()
        projs = []

        for exp in self.select_exprs:
            if exp.alias is not None:
                projs.append(exp.alias)
                select_expr[exp.alias] = exp
            else:
                projs.append(str(exp.term))
                select_expr[str(exp.term)] = exp
        projs = sorted(projs)
        select_exprs = [select_expr.get(p) for p in projs]
        self.select_exprs = None
        self.select_exprs= select_exprs
        return self

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.distinct == other.distinct and \
                len(set(self.select_exprs)) == len(set(self.select_exprs).intersection(other.select_exprs)) and \
                len(set(self.where_exprs)) == len(set(self.where_exprs).intersection(other.where_exprs))  and \
                len(set(self.from_exprs)) == len(set(self.from_exprs).intersection(other.from_exprs))

    def __hash__(self):
        return hash(self.__str__())


class SQLSelectExpression:
    def __init__(self, term, alias=None):
        self.term = term
        self.alias = alias

    def __str__(self):
        alias = "" if self.alias is None else " AS `" + self.alias + '`'
        return str(self.term) + alias

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.term == other.term and (self.alias is None and other.alias is None or self.alias == other.alias)

    def __hash__(self):
        return hash(self.__str__())


class SQLWildcardExpression:
    def __init__(self, table_alias=None):
        self.table_alias = table_alias

    def __str__(self):
        prefix = "" if self.table_alias is None else (self.table_alias + '.')
        return prefix + '*'

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.table_alias == other.table_alias


class SQLColumn:
    def __init__(self, name, table_alias=None, schema=None):
        self.name = name
        self.table_alias = table_alias
        self.schema = schema

    def __eq__(self, other):
        return self.name == other.name and self.table_alias == other.table_alias and self.schema == other.schema

    def __str__(self):
        alias = ''
        if self.table_alias is not None:
            alias += self.table_alias + '.'
        if self.schema is not None:
            alias = self.schema + '.' + alias
        try:
            return alias + self.name
        except Exception as e:
            print('Exception', e)
            print(alias, self.name)
            return alias + self.name

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__str__())


class SQLFunction:
    def __init__(self, name, args, columns=List[SQLColumn], distinct=False):
        self.name = name
        self.args = args
        if self.args is None:
            self.args = []
        self.distinct = distinct
        self.columns = columns
        if self.columns is None:
            self.columns = []

    def __eq__(self, other):
        return self.name == other.name and \
                self.distinct == other.distinct and \
                len(set(self.columns)) == len(set(self.columns).intersection(other.columns)) and \
                len(set(self.args)) == len(set(self.args).intersection(other.args))

    def __str__(self):
        dist = ''
        if self.distinct:
            dist = "DISTINCT "
        return self.name + '(' + dist + ",".join(self.args) + ')'

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__str__())


class SQLFromExpression:
    def __init__(self, table, alias=None):
        self.table = table
        self.alias = alias

    def __eq__(self, other):
        return self.table == other.table and (self.alias is None and other.alias is None or self.alias == other.alias)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        sql = str(self.table)
        if isinstance(self.table, SQLTable):
            alias = sql if self.alias is None else sql + " AS " + self.alias
        else:
            alias = sql if self.alias is None else '(' + sql + ") AS " + self.alias
        return alias

    def __hash__(self):
        return hash(self.__str__())


class SQLTable:
    def __init__(self, name, schema=None):
        self.name = name
        self.schema = schema

    def __eq__(self, other):
        return self.name == other.name and (self.schema is None and other.schema is None or self.schema == other.schema)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        sch = ''
        if self.schema is not None:
            sch = self.schema + "."

        return sch + self.name

    def __hash__(self):
        return hash(self.__str__())


class SQLSubQuery:
    def __init__(self, sub_query, alias=None):
        self.sub_query = sub_query
        self.alias = alias

    def __eq__(self, other):
        return self.sub_query == other.sub_query and (self.alias is None and other.alias is None or self.alias == other.alias)

    def sort_projections(self):
        self.sub_query = self.sub_query.sort_projections()
        return self

    def __str__(self):

        if self.alias is not None:
            alias = " AS " + self.alias
            return '(' + self.sub_query + ") " + alias
        else:
            return self.sub_query

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__str__())


class SQLUnions:
    def __init__(self, sub_queries):
        self.sub_queries = sub_queries
        # self.sort_projections()
        from pprint import pprint
        pprint(self.sub_queries)

    def sort_projections(self):
        sub_queries = []
        for sql in self.sub_queries:
            sql = sql.sort_projections()
            sub_queries.append(sql)
        self.sub_queries = sub_queries
        return self

    def __str__(self):
        return "\n UNION \n".join([str(subq) for subq in self.sub_queries])

    def __repr__(self):
        return self.__str__()


class SQLAndCondition:
    def __init__(self, conditions: List):
        self.conditions = conditions  # list(set(conditions))

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        self.conditions = list(set(self.conditions))
        return " AND ".join([str(c) for c in self.conditions])

    def __eq__(self, other):
        return len(set(self.conditions).intersection(other.conditions)) < len(set(self.conditions))

    def __hash__(self):
        return hash(self.__str__())


class SQLOrCondition:
    def __init__(self, conditions: List):
        self.conditions = list(set(conditions))

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '(' + (" OR ".join([str(c) for c in self.conditions])) + ')'

    def __eq__(self, other):
        return len(set(self.conditions).intersection(other.conditions)) < len(set(self.conditions))

    def __hash__(self):
        return hash(self.__str__())


class SQLCondition:
    def __init__(self, left, op=None, right=None):
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.op is None and self.right is None:
            return str(self.left)
        if self.right is None:
            return str(self.op) + ' ' + str(self.left)

        return str(self.left) + ' ' + str(self.op) + ' ' + str(self.right)

    def __eq__(self, other):
        if self.op is None:
            return self.left == other.left and other.op is None
        elif self.right is None:
            return self.left == other.left and self.op == other.op and other.right is None
        else:
            return self.left == other.left and self.op == other.op and self.right == other.right

    def __hash__(self):
        return hash(self.__str__())


class SQLJoins:
    def __init__(self,
                 base_query,
                 joining_queries: Dict,
                 join_conditions,
                 base_query_alias='base_query',
                 projections=None):

        self.base_query = base_query
        self.joining_queries = joining_queries
        self.join_conditions = join_conditions
        self.projections = projections
        if self.projections is None:
            self.projections = [SQLSelectExpression(SQLColumn('*'))]

        self.base_query_alias = base_query_alias
        self.sql = self._get_sql()

    def _get_sql(self):
        sql = SQLSelectQuery(True)
        if self.projections is not None:
            sql.select_exprs = self.projections

        sql.from_exprs = [SQLFromExpression(self.base_query, self.base_query_alias)]
        for joining_query_alias, joining_query in self.joining_queries.items():
            sql.from_exprs.append(SQLFromExpression(joining_query, joining_query_alias))

        sql.where_exprs = [self.join_conditions]

        return sql

    def __str__(self):
        return str(self.sql)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.__str__())
