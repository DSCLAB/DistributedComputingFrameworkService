import logging
import traceback
import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine
import json
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import happybase
import subprocess
import urllib.parse
import pika, sys
import time
import numpy as np
import phoenixdb
import phoenixdb.cursor
import happybase

def get_column_names(db: dict):
    try:
        DbType = db['ConnectionInfo']['DbType']
        Ip = db['ConnectionInfo']['Ip']
        Port = db['ConnectionInfo']['Port']
        UserName = db['ConnectionInfo']['UserName']
        Password = db['ConnectionInfo']['Password']
        DbName = db['DbInfo']['DbName']
        TableName = db['DbInfo']['TableName'] if ("DbInfo" in db and "TableName" in db["DbInfo"]) else ""
        logging.info('Start import metadata of DB')
        #send_task_status(task_id, TASKSTATUS_PROCESSING, f"Start import metadata of DB:{DbType}")
        if DbType == 'mysql':
            sql_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{TableName}'"
            engine = create_engine(f"mysql+pymysql://{UserName}:{Password}@{Ip}:{Port}/{DbName}")
            conn = engine.connect()
            result = conn.execute(sql_query)
            col_list = [row[0] for row in result]
            conn.close()
        elif DbType == 'mssql':
            table_parts = TableName.split('.')
            TABLE_SCHEMA = table_parts[0].replace('[', '').replace(']', '')
            TABLE_NAME = table_parts[1].replace('[', '').replace(']', '')
            sql_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{TABLE_SCHEMA}' AND TABLE_NAME = '{TABLE_NAME}'"
            engine = create_engine(f"mssql+pymssql://{UserName}:{Password}@{Ip}:{Port}/{DbName}")
            conn = engine.connect()
            result = conn.execute(sql_query)
            col_list = [row[0] for row in result]
            conn.close()
        elif DbType == 'phoenix':
                conn = phoenixdb.connect('http://%s:%s' % (Ip, Port))
                cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
                cursor.execute("SELECT column_name FROM system.catalog WHERE table_name = '%s' AND column_name IS NOT NULL" % TableName)
                res = cursor.fetchall()
                col_list = [item['COLUMN_NAME'] for item in res]
        elif DbType == 'oracle':
            sql_query = f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = '{DbName}' AND TABLE_NAME = '{TableName}'"
            engine = create_engine(f"oracle+cx_oracle://{UserName}:{Password}@{Ip}:{Port}")
            conn = engine.connect()
            result = conn.execute(sql_query)
            col_list = [row[0] for row in result.fetchall()]
            conn.close()
        elif DbType == 'cassandra':
            keyspace_name = DbName
            auth_provider = PlainTextAuthProvider(UserName, Password)
            cluster = Cluster([Ip], port=int(Port), auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(keyspace_name)
            result = session.execute(f"DESCRIBE TABLE {TableName}")
            col_list = result.column_names
            session.shutdown()
            cluster.shutdown()
        elif DbType == 'elasticsearch':
            es = Elasticsearch(hosts=Ip, port=Port, http_auth=(UserName, Password))
            doc_type = TableName
            mapping = es.indices.get_mapping(index = doc_type)
            keys = []
            for index in mapping:
                for layer1 in mapping[index]['mappings']['properties']:
                    if 'properties' in mapping[index]['mappings']['properties'][layer1]:
                        for layer2 in mapping[index]['mappings']['properties'][layer1]['properties']:
                            keys.append(layer1 + '.' + layer2)
                    else:
                        keys.append(layer1)
            col_list = list(dict.fromkeys(keys))
            es.close()
        elif DbType == 'mongodb':
            mongo_client = MongoClient(f'mongodb://{UserName}:{Password}@{Ip}:{Port}/')
            db = mongo_client[DbName]
            collection = db[TableName]
            col_list = list(collection.find_one().keys())
        elif DbType == 'csv':
            hds_host = "hbase-regionserver1"
            hds_port = "8000"
            params = {'from': f'{DbName}{TableName}', 'to': 'local:///'}
            encoded = urllib.parse.urlencode(params)
            url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}'
            df = pd.read_csv(url)
            col_list = df.columns.tolist()   
        elif DbType == 'hbase': 
            connection = happybase.Connection(Ip, port=int(Port))
            table = happybase.Table(TableName, connection)
            qualifiersSet = set()
            for _, valuex in table.scan():
                for keyy, _ in valuex.items():
                    qualifiersSet.add(keyy.decode("utf-8"))
            connection.close()
            col_list = list(qualifiersSet)
        elif DbType == 'excel' or DbType == 'hds' or DbType == 'None':
            col_list = db["DbInfo"]["ColumnsName"]
        logging.debug('col_list:' + str(col_list))
        logging.info(f"Finished import metadata of DB:{DbType}")
        #send_task_status(task_id, TASKSTATUS_PROCESSING, f"Finished import metadata of DB:{DbType}")
        #return col_list



        metadata = {
            "DbType": DbType,
            "Ip": Ip,
            "Port": Port,
            "UserName": UserName,
            "Password": Password,
            "DbName": DbName,
            "TableName": TableName,
            "ColumnsName": col_list
        }
        return metadata

    except Exception as e:
        logging.error(f"error when git metadata from {DbType}:" + traceback.format_exc())
        #send_task_status(task_id, TASKSTATUS_FAILED, f"error when git metadata from {DbType}:" + str(e))
        exit(1)

def store_metadata_to_elasticsearch(metadata_list):
    es = Elasticsearch(['elasticsearch:9200'],http_auth=('elastic','elastic'))

    index_name = "metadata"
    doc_type = "_doc"

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    documents = [
        {
            "_index": index_name,
            "_id": f"{metadata['Ip']}:{metadata['Port']}:{metadata['DbName']}:{metadata['TableName']}",
            "_source": metadata
        } for metadata in metadata_list
    ]

    try:
        helpers.bulk(es, documents)
    except Exception as e:
        logging.error(f"Failed to store data into elk: {str(e)}")
        es.close()
        return e

    es.close()