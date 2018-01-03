# -*- encoding:UTF-8 -*-

import sys
import json
import uuid
from pyspark import SparkConf
from pyspark import  SparkContext
from operator import add
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import time
from kafka import KafkaProducer

from CassandraDAO import CassandraDAO,CassandraType

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement

from pyspark.sql import Row, SparkSession, SQLContext, types
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
import requests
from ConfigParser import RawConfigParser


reload(sys)
sys.setdefaultencoding('utf-8')


def GetConfig(setting):
        config = RawConfigParser()
        config.read('/app/config.cfg')
        single_section = config.items(setting)
        print('get config (%s): %s' %(setting, single_section[0][1]))
        return single_section[0][1]

def CreateSparkContext(IP):
    sparkConf = SparkConf() \
                         .setAppName('PythonStreaming').setMaster('local[2]') \
                         .set('spark.cassandra.connection.host', IP)
    sc = SparkContext(conf = sparkConf)
    return (sc)

def getSparkSessionInstance(sparkConf):
    try:
        if ('sparkSessionSingletonInstance' not in globals()):
            globals()['sparkSessionSingletonInstance'] = SparkSession\
                .builder\
                .config(conf=sparkConf)\
                .master('local').getOrCreate()
        return globals()['sparkSessionSingletonInstance']
    except Exception,ee:
        print(ee)
    finally:
        pass

def handlingPreviousMsg():
    try:
        cluster = Cluster()
        cluster = Cluster([cassandra_IP])
        session = cluster.connect(cassandra_keyspace)
        insert_records = session.prepare('INSERT INTO '+cassandra_table + '(create_date, create_user, raw_data) VALUES (?,?,?)')
        batch = BatchStatement()  # default is ATOMIC ( All or nothing)
        guid = ''
        error_msgs = []

        # get error messages by api
        params = {
            'topic':topic,
            'group':topic+'-consumer'
        }
        url=kafka_handling_api+'/receive_error_msg'

        response=requests.post(url,json=params)
        print(response.text)

        print('response.status_code='+str(response.status_code))

        # handle error msg
        if response.status_code == 200:


            r = json.loads(response.text)
            print(r)
            guid = r['guid']

            msgs = list(r['topic_messages'])

            for item in msgs:
                batch.add(insert_records, (str(datetime.now()),'api',item.encode('utf-8')))

        else:
            printError(url, response.text)

        session.execute(batch)

        # Commit msg after insert to cassandra successfully

        url2=kafka_handling_api+'/commit_error_msg'

        params2 = {
            'topic':topic,
            'guid':guid,
            'group':topic+'-consumer'
        }

        response2=requests.post(url2,json=params2)
        print(response2.text)
        print(response2.status_code)

    except Exception,ee2:
        print('error when handlingPreviousMsg : ' + str(ee2))
    finally:
        print('finished handlingPreviousMsg...')

def printError(function_name, e):
        print('******************* Print Error *******************')
        print('Function Name or API: %s' % (function_name))
        print('Error: %s' % (e))
        print('***************************************************')


# Convert RDDs of the words DStream to DataFrame and run SQL query
def saveToCassandra(sc, ssc, rowRdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rowRdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rdd = rowRdd.map(lambda w: Row(create_date=w[0], create_user=w[1], raw_data=w[2]))

        schema = StructType([
                StructField('create_date', StringType(), True),
                StructField('create_user', StringType(), True),
                StructField('raw_data', StringType(), True)
        ])

        wordsDataFrame = spark.createDataFrame(rdd, schema=schema)

        wordsDataFrame.show()

        c_dao.saveToCassandraDF(wordsDataFrame, cassandra_keyspace, cassandra_table, 'append')

    except Exception,ee2:
        HandleError(rowRdd, str(ee2))
        print(ee2)
    finally:
        pass


def HandleError(rowRdd, exception_msg):
    try:
        print('================================handle error ================================')

        record_id = str(uuid.uuid4())
        lstRowRdd = rowRdd.collect()
        cqllist=[]

        if lstRowRdd > 0:

            for iRowRdd in lstRowRdd:
                input_data = str(iRowRdd[2])  #.encode('utf-8')
                print(type(input_data))
                print(input_data.encode('utf-8'))
                exception_msg2 = exception_msg.replace("'","''")
                cql_command = "insert into "+ cassandra_keyspace +".spark_streaming_error_log (random_id, topic, message, create_date, remark, processed) VALUES ('"+ record_id +"', '"+ topic +"', '"+ input_data +"', '"+ (str(datetime.now())) +"' ,'"+ exception_msg2 +"', 'N')"
                cqllist.append(cql_command)

            print('let''s print cqllist')
            print(cqllist)
            c_dao.BatchInsertIntoCassandra(cqllist, cassandra_keyspace)
            #session.execute(batch)
        else:
            pass

    except Exception, e:
        print('Catch exception when HandleError:'+str(e))
    finally:
        pass

def main():

    global zookeeper_IP
    global cassandra_IP
    global cassandra_keyspace
    global cassandra_table
    global kafka_handling_api
    global seconds_per_job
    global topic
    global c_dao

    zookeeper_IP=GetConfig('zookeeper_IP')
    cassandra_IP =GetConfig('cassandra_IP')
    cassandra_keyspace=GetConfig('cassandra_keyspace')
    cassandra_table=GetConfig('cassandra_table')
    kafka_handling_api=GetConfig('kafka_handling_api')
    seconds_per_job=GetConfig('seconds_per_job')
    topic=GetConfig('topic')
    c_dao = CassandraDAO(CassandraType.PRODUCTION)

    sc = CreateSparkContext(cassandra_IP)
    ssc = StreamingContext(sc, int(float(seconds_per_job)))

    try:

        kafka_stream = KafkaUtils.createStream(ssc, zookeeper_IP, 'spark-streaming-consumer', {topic:12})
        raw = kafka_stream.flatMap(lambda kafkaS: [kafkaS])
        lines = raw.filter(lambda xs: xs[1].split(','))

        counts = lines.map(lambda word: (str(datetime.now()), 'api', word[1]))
        counts.foreachRDD(lambda k: saveToCassandra(sc, ssc, k))

    except Exception, e:
        print('error:'+str(e))
    finally:
        ssc.start()
        ssc.awaitTermination()

if __name__ == '__main__':
        main()
