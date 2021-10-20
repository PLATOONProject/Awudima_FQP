__author__ = 'Kemele M. Endris'

from mysql import connector
from mysql.connector import errorcode
from queue import Empty
from multiprocessing import Process, Queue
from requests.utils import requote_uri
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MySQLClient(object):
    def __init__(self, url=None, username=None, passwd=None):
        self.url = url
        self.username = username
        self.password = passwd

        if self.url is None:
            self.url = '127.0.0.1'

        if ':' in self.url:
            host, port = self.url.split(':')
        else:
            host = self.url
            port = '3306'

        try:
            if self.username is None:
                self.client = connector.connect(user='root', host=self.url)
            else:
                self.client = connector.connect(user=username, password=passwd, host=host, port=port)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def list_tables(self):
        results = []
        cursor = self.client.cursor()
        cursor.execute("show databases")
        databases = []
        for d in cursor:
            if d[0] != 'information_schema' and d[0] != 'mysql' and d[0] != "sys" and d[0] != 'performance_schema':
                databases.append(d[0])
        for d in databases:
            cursor.execute("use " + d)
            cursor.execute("show tables")

            tables = [t[0] for t in cursor]

            counter = self.client.cursor()
            counter.execute('use ' + d)
            for t in tables:
                counter.execute('SELECT COUNT(*) FROM ' + t)
                count = [c[0] for c in counter][0]
                row = {
                    "db": d,
                    "document": t,
                    "count": count
                }
                results.append(row)
        return results

    def get_samples(self, dbname, tablename, limit=10):
        cursor = self.client.cursor()
        cursor.execute("use " + dbname)
        cursor.execute("select * from " + tablename + " LIMIT " + str(limit))
        header = [h[0] for h in cursor._description]
        results = [{header[i]: str(line[i]) for i in range(len(line))} for line in cursor]
        return results, len(results)


class MySQLWrapper:
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
        if ':' in self.url:
            self.host, self.port = self.url.split(':')
        else:
            self.host = self.url
            self.port = '3306'

    def init_connection(self):
        try:
            if self.username is None:
                mysql = connector.connect(user='root',
                                          host=self.url)
            else:
                mysql = connector.connect(user=self.username,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port)
        except connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                logger.error("Database does not exist")
            else:
                print(err)
                logger.error("Error:" + str(err))
            return None
        except Exception as ex:
            print("Exception while connecting to Mysql", ex)
            logger.error("Exception while connecting to Mysql" + str(ex))
            return None

        return mysql

    def executeQuery(self, sql_query, queue, sparql_result_template, limit=-1, offset=0):
        if self.database_name is None:
            queue.put('EOF')
            return
        mysql = self.init_connection()
        if mysql is None:
            queue.put('EOF')
            return

        cursor = mysql.cursor()
        cursor.execute("use " + self.database_name + ';')
        card = 0
        if limit == -1:
            limit = 10000
        if offset == -1:
            offset = 0

        logger.info(sql_query)
        try:
            # rs = time()
            while True:
                query_copy = str(sql_query) + "\n LIMIT " + str(limit) + " OFFSET " + str(offset)
                # print("_______________________")
                # print(query_copy)
                cursor.execute(query_copy)
                cardinality = self.process_result(cursor, queue, sparql_result_template)
                card += cardinality
                # if (time()-rs) > 20:
                #     print(card, 'results found -..')
                if cardinality < limit:
                    break
                offset = offset + limit
            logger.info("Running query: " + str(sql_query) + " Non UNION DONE" + str(card))
        except IOError as ie:
            print("IO ERROR:", ie)
            logger.error("IOError while running query: " + str(ie))
            pass
        except Exception as e:
            print("EXception: ", e)
            logger.error("Exception while running query: " + str(e))
            pass

        queue.put("EOF")

    def process_result(self, cursor, queue, sparql_result_tempalte: dict):
        header = [h for h in cursor.column_names]
        c = 0
        for line in cursor:
            c += 1
            row = {}
            res = sparql_result_tempalte.copy()
            skip = False
            for i in range(len(line)):
                proj_var = header[i]
                row[proj_var] = str(line[i])

                if row[proj_var] == 'null':
                    skip = True
                    break

                if proj_var in sparql_result_tempalte:
                    value = str(row[proj_var])
                    try:
                        if res[proj_var]['type'] == 'uri':
                            value = requote_uri(value)
                        if res[proj_var]['type'] == 'bnode':
                            value = '_:' + requote_uri(value)
                        res[proj_var]['value'] = value
                    except Exception as e:
                        print(value)
                        print("Ex: ", e)
                        skip = True
                        break
            if not skip:
                queue.put(res)
        return c
