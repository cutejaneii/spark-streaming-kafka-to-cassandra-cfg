# encoding=UTF-8
#!flask/bin/python

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
import pandas as pd
from pyspark.sql.types import StructType

class CassandraType(object):
    PRODUCTION = 0
    TEST = 1
    TEST_DOCKER = 2

class CassandraDAO(object):

    # you have to install following items :
    # a. python-Cassandra Connector
    # b. sqlJDBC.jars

    def __init__(self, type):
#         print('runing father.__init__')
        if type == CassandraType.PRODUCTION:
            self.contact_points=['192.168.0.1','192.168.0.2']
            self.contact_points_str = "192.168.0.1,192.168.0.2"
        elif type == CassandraType.TEST:
            self.contact_points=['192.168.0.3','192.168.0.4']
            self.contact_points_str = "192.168.0.3,192.168.0.4"
        else:
            self.contact_points=['192.168.0.5','192.168.0.6','192.168.0.7']
            self.contact_points_str = "192.168.0.5,192.168.0.6,192.168.0.7"

        self.formatString = "org.apache.spark.sql.cassandra"
        self.username = "username"
        self.password = "password"
        self.cluster = None
        self.session = None
        self.createSession()

    def __del__(self):
        self.cluster.shutdown()

    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def createSession(self):
        print "contact_points = " + self.contact_points_str
        self.cluster = Cluster(
            contact_points=self.contact_points, #random select a node
        )
        self.session = self.cluster.connect()
        self.session.row_factory = self.pandas_factory
        self.session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.  

    def getSession(self):
        return self.session

    def execCQL(self, keyspace, cql):
        """
        execute CQL
        """
        self.session.set_keyspace(keyspace)
        self.session.execute_async(cql)

    def execCQLSelect(self, keyspace, cql):
        """
        execute CQL, select only
        """

        self.session.set_keyspace(keyspace)

#       cassandra ResultSet
        async_results = self.session.execute_async(cql)
        return async_results

    def execCQLCallBackAnysc(self, keyspace, cql, handle_success, handle_error):
        """
        execute CQL, if success => handle_success function, else handle_error
        """
        self.session.set_keyspace(keyspace)
        async_results = self.session.execute_async(cql)
        async_results.add_callbacks(handle_success, handle_error)

    def execCQLSelectToPandasDF(self, keyspace, cql):
        """
        execute CQL, select only, return Pandas DataFrame
        """

        self.session.set_keyspace(keyspace)

#       cassandra ResultSet
        async_results = self.session.execute_async(cql)
#       to Pandas DataFrame
        return async_results.result()._current_rows

    def execCQLSelectToDF(self, sqlContext, keyspace, cql):
        """
        execute CQL, select only, return Spark DataFrame
        """

#       pandas dataframe to spark dataframe
        pandas_dataframe = self.execCQLSelectToPandasDF(keyspace, cql)
        if pandas_dataframe.empty:
            schema = StructType([])
            return sqlContext.createDataFrame([],schema)
        else:
            return sqlContext.createDataFrame(pandas_dataframe)

    def execCQLSelectToRDD(self, sqlContext, keyspace, cql):
        """
        execute CQL, select only, return Spark RDD
        """

        return self.execCQLSelectToDF(sqlContext, keyspace, cql).rdd.map(tuple)#dataFrame to RDD

    @property
    def contactPoints(self):
        return self.contact_points

    @contactPoints.setter
    def contactPoints(self, contact_points):
        self.contact_points = contact_points

    @contactPoints.deleter
    def contactPoints(self):
        del self.contact_points

# pyspark cassandra connector
    def readFromCassandraDF(self, sqlContext, keyspace, table):
        """
        read data from Cassandra, return Dataframe
        """

        return sqlContext.read\
                        .format(self.formatString)\
                        .options(table=table, keyspace=keyspace)\
                        .option("spark.cassandra.connection.host",self.contact_points_str)\
                        .load()

    def readFromCassandraRDD(self, sqlContext, keyspace, table):
        """
        read data from Cassandra, return RDD
        """

        df =  sqlContext.read\
                        .format(self.formatString)\
                        .options(table=table, keyspace=keyspace)\
                        .option("spark.cassandra.connection.host",self.contact_points_str)\
                        .load()
        return df.rdd.map(tuple)#dataFrame to RDD

    def saveToCassandraDF(self, dataFrame, keyspace, table, mode="error"):
        """
        Save data to Cassandra using DataFrame, select one mode to save

        SaveMode.ErrorIfExists (default) | "error"      When saving a DataFrame to a data source,
                                                        if data already exists, an exception is expected to be thrown.
        SaveMode.Append                  | "append"     When saving a DataFrame to a data source,
                                                        if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
        SaveMode.Overwrite               | "overwrite"  Overwrite mode means that when saving a DataFrame to a data source,
                                                        if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
        SaveMode.Ignore                  | "ignore"     Ignore mode means that when saving a DataFrame to a data source,
                                                        if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.
        """

        dataFrame.write\
                .format(self.formatString)\
                .mode(mode)\
                .options(table=table, keyspace=keyspace)\
                .option("spark.cassandra.connection.host",self.contact_points_str)\
                .save()

    def BatchInsertIntoCassandra(self, cqllist, keyspace):
        """
        Batch insert into Cassandra
        """
        self.cluster = Cluster(
            contact_points=self.contact_points,
        )
        self.session = self.cluster.connect()

        batch = BatchStatement()  # default is ATOMIC ( All or nothing)

        if cqllist > 0:

            for cql_command in cqllist:
                batch.add(SimpleStatement(cql_command))

            self.session.execute(batch)
        else:
            pass

