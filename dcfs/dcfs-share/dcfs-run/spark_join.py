from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql.window import Window

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.functions import lit
from pyspark.sql.functions import mean as _mean
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col

from urllib.request import urlopen
import sys
import logging
import traceback
from datetime import datetime, timedelta
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
from neo4j import GraphDatabase
from math import log

import subprocess
import urllib.parse
import pika, sys
import time
import os
import os.path

import numpy as np
import phoenixdb
import phoenixdb.cursor
from collections import Counter
from collections import defaultdict

start = time.time()

sparkSession = SparkSession.builder.appName('spark join app').getOrCreate()

log_dir = "/dcfs-share/spark-task-logs/"
rmq_host = "rabbitmq"
hds_host = "hbase-regionserver1"
hds_port = "8000"
tmp_dir = "/tmp/dcfs"

''' ========== logging ========== '''
def setup_logging(filename):
    logging.basicConfig(level=logging.DEBUG,
                    # format='%(asctime)s - %(levelname)s - %(message)s',
                    format='%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n',
                    filename=log_dir+filename,
                    filemode='w')
    # Until here logs only to file: 'logs_file'

    # define a new Handler to log to console as well
    console = logging.StreamHandler()
    # optional, set the logging level
    console.setLevel(logging.DEBUG)
    # set a format which is the same for console use
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)
    # Set all other logger levels to WARNING
    for logger_name in logging.root.manager.loggerDict:
        if logger_name != __name__:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

''' ========== HADOOP remove dataframe file(df0, df1) ========== '''
def remove_hadoop_df_csv(df_index):
    process = subprocess.Popen("/opt/hadoop-3.3.4/bin/hadoop fs -rm -skipTrash /join/" + f"df{df_index}_{task_id}_{ts}.csv", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    logging.debug(f"hadoop remove tmp df{df_index} file. stdout:{stdout}, stderr:{stderr}")

''' ========== RabbitMQ ========== '''
TASKSTATUS_ACCEPTED   = 1
TASKSTATUS_PROCESSING = 2
TASKSTATUS_SUCCEEDED  = 3
TASKSTATUS_FAILED     = 4
TASKSTATUS_ERROR      = 5
TASKSTATUS_UNKNOWN    = 6

def send_task_status(task_id, status, message, link='', table_name=''):
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='task_status')
    payload = {
        'task_id': task_id,
        'status':  status,
        'message': message,
        'link':    link,
        'table_name': table_name
    }
    body = json.dumps(payload)
    channel.basic_publish(exchange='', routing_key='task_status', body=body)
    connection.close()

''' ========== datasource function  ========== '''
def sum_function(df_index, function_info):
    if "GroupBy" in function_info and function_info["GroupBy"]["IsEnable"] == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        tmp_group_df = globals()[f'df{df_index}'].groupBy(groupBy_column).agg(_sum(function_info["TargetColumn"]).alias(function_info["Naming"]))
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].join(tmp_group_df, on=[groupBy_column])
        tmp_group_df = None
    else:
        sum_value = globals()[f'df{df_index}'].agg(_sum(function_info["TargetColumn"])).first()[0]
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], lit(sum_value))

def avg_function(df_index, function_info):
    if "GroupBy" in function_info and function_info["GroupBy"]["IsEnable"] == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        tmp_group_df = globals()[f'df{df_index}'].groupBy(groupBy_column).agg(_mean(function_info["TargetColumn"]).alias(function_info["Naming"]))
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].join(tmp_group_df, on=[groupBy_column])
        tmp_group_df = None
    else:
        mean_value = globals()[f'df{df_index}'].agg(_mean(function_info["TargetColumn"])).first()[0] # 4.0
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], lit(mean_value))

def mode_function(df_index, function_info):
    mode_value = globals()[f'df{df_index}'].groupBy(function_info["TargetColumn"]).count().orderBy("count", ascending=False).first()[0]
    globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], lit(mode_value))

def columncombine_function(df_index, function_info):
    targetColumnList = function_info['TargetColumnList']
    def column_order(elem):
        return elem["Order"]
    targetColumnList.sort(key=column_order)
    for combine_idx, combine_order in enumerate(targetColumnList):
        if combine_idx == 0:
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(combine_order["TargetColumn"]))
        else:
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.concat(sf.col(function_info["Naming"]), sf.lit(function_info["Separator"]), sf.col(combine_order["TargetColumn"])))

def pivot_function(df_index, function_info):
    targetColumn = function_info["TargetColumn"]
    aggregateColumn = function_info["AggregateColumn"][0]["Column"]
    aggregateFunction = function_info["AggregateFunction"]
    isGroupBy = function_info["GroupBy"]["IsEnable"]
    if isGroupBy == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        if aggregateFunction == "MAX":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(groupBy_column).pivot(targetColumn).agg(sf.max(sf.col(aggregateColumn)))
        elif aggregateFunction == "SUM":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(groupBy_column).pivot(targetColumn).agg(_sum(sf.col(aggregateColumn)))
        elif aggregateFunction == "AVG":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(groupBy_column).pivot(targetColumn).agg(_mean(sf.col(aggregateColumn)))
    else:
        if aggregateFunction == "MAX":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(targetColumn).pivot(targetColumn).agg(sf.max(sf.col(aggregateColumn))).drop(targetColumn)
        elif aggregateFunction == "SUM":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(targetColumn).pivot(targetColumn).agg(_sum(sf.col(aggregateColumn))).drop(targetColumn)
        elif aggregateFunction == "AVG":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupBy(targetColumn).pivot(targetColumn).agg(_mean(sf.col(aggregateColumn))).drop(targetColumn)

def arithmetic_function(df_index, function_info):
    operator = function_info["Operator"]
    targetColumnList = function_info['TargetColumnList']
    length_targetColumnList = len(targetColumnList)
    if length_targetColumnList <=1: return
    def column_order(elem):
        return elem["Order"]
    targetColumnList.sort(key=column_order)
    if operator == "ADD":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(arithmetic_order["Column"]))
            else:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(function_info["Naming"])+sf.col(arithmetic_order["Column"]) )
    
    if operator == "SUB":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(arithmetic_order["Column"]))
            else:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(function_info["Naming"])-sf.col(arithmetic_order["Column"]) )
    
    if operator == "MUL":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(arithmetic_order["Column"]))
            else:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(function_info["Naming"])*sf.col(arithmetic_order["Column"]) )
    
    if operator == "DIV":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(arithmetic_order["Column"]))
            else:
                globals()[f'df{df_index}'] = globals()[f'df{df_index}'].withColumn(function_info["Naming"], sf.col(function_info["Naming"])/sf.col(arithmetic_order["Column"]) )

def datasource_function(df_index, functionCondition):
    start_column_operarion_time = time.time()
    logging.info('Start column operarion time')
    # take Step element for sort
    def takeStep(elem):
        return elem[1]["Step"]
    execute_order = []
    toSum = "ToSum"
    toAvg = "ToAvg"
    toMode = "ToMode"
    toColumnCombine = "ToColumnCombine"
    toPivot = "ToPivot"
    toArithmetic = "ToArithmetic"
    if toSum in functionCondition:
        for function_sum in functionCondition[toSum]:
            if function_sum["IsEnable"] == "True":
                execute_order.append((toSum, function_sum))
    if toAvg in functionCondition:
        for function_avg in functionCondition[toAvg]:
            if function_avg["IsEnable"] == "True":
                execute_order.append((toAvg, function_avg))
    if toMode in functionCondition:
        for function_mode in functionCondition[toMode]:
            if function_mode["IsEnable"] == "True":
                execute_order.append((toMode, function_mode))
    if toColumnCombine in functionCondition:
        for function_columncombine in functionCondition[toColumnCombine]:
            if function_columncombine["IsEnable"] == "True":
                execute_order.append((toColumnCombine, function_columncombine))
    if toPivot in functionCondition:
        for function_pivot in functionCondition[toPivot]:
            if function_pivot["IsEnable"] == "True":
                execute_order.append((toPivot, function_pivot))
    if toArithmetic in functionCondition:
        for function_arithmetic in functionCondition[toArithmetic]:
            if function_arithmetic["IsEnable"] == "True":
                execute_order.append((toArithmetic, function_arithmetic))
    execute_order.sort(key=takeStep)
    for execute_function in execute_order:
        if execute_function[0] == toSum:
            sum_function(df_index, execute_function[1])
        if execute_function[0] == toAvg:
            avg_function(df_index, execute_function[1])
        if execute_function[0] == toMode:
            mode_function(df_index, execute_function[1])
        if execute_function[0] == toColumnCombine:
            columncombine_function(df_index, execute_function[1])
        if execute_function[0] == toPivot:
            pivot_function(df_index, execute_function[1])
        if execute_function[0] == toArithmetic:
            arithmetic_function(df_index, execute_function[1])
    end_column_operarion_time = time.time()
    logging.info(f"Finished column operarion time. Time:{str(end_column_operarion_time - start_column_operarion_time)}")

''' ========== special function  ========== '''
def split_merge_function(df_index, function_info):
    new_lot = function_info['TargetColumn']['NewLot']
    old_lot = function_info['TargetColumn']['OldLot']
    # Get distinct values of the 'NewLot' column and 'NewLot' column copy to 'OldLot' column
    tmp_distinct_newlots = globals()[f'df{df_index}'].select(new_lot).distinct().withColumn(old_lot, sf.col(new_lot))
    globals()[f'df{df_index}'] = globals()[f'df{df_index}'].union(tmp_distinct_newlots)
    tmp_distinct_newlots = None

def special_function(df_index, special_function):
    # take Step element for sort
    if special_function is None:
        return
    start_special_column_operarion_time = time.time()
    logging.info('Start special operarion time')
    def takeStep(elem):
        return elem[1]["Step"]
    execute_order = []
    splitMerge = "SplitMerge"
    if splitMerge in special_function:
        for function_split_merge in special_function[splitMerge]:
            if function_split_merge["IsEnable"] == "True":
                execute_order.append((splitMerge, function_split_merge))
    execute_order.sort(key=takeStep)
    for execute_function in execute_order:
        if execute_function[0] == splitMerge:
            split_merge_function(df_index, execute_function[1])
    end_special_column_operarion_time = time.time()
    logging.info(f"Finished special operarion time. Time:{str(end_special_column_operarion_time - start_special_column_operarion_time)}")

''' ========== retrieve data ========== '''
def get_join_data(task_id, task_dict):
    try:
        logging.info("Start retrieveing data.")
        tmp_dir = "/tmp/dcfs"

        os.makedirs(tmp_dir, exist_ok=True)
        cmd = '/opt/hadoop-3.3.4/bin/hadoop fs -mkdir /join'
        process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        process.wait()
        stdout = stdout.decode('utf-8')
        stderr = stderr.decode('utf-8')
        logging.debug(f"/opt/hadoop-3.3.4/bin/hadoop fs -mkdir /join:stdout:{stdout}, stderr:{stderr}")
    except Exception as e:
        logging.error("Error in setup retrieving data: " + traceback.format_exc())
        send_task_status(task_id, TASKSTATUS_FAILED, "Error in setup env for retrieving: " + str(e))
        exit(1)

    for index, db in enumerate(task_dict['Base']['Databases']):
        try:
            start_retrieve_time = time.time()

            db_connection = db['ConnectionInfo']
            db_type = db_connection['DbType']
            db_info = db['DbInfo']
            send_task_status(task_id, TASKSTATUS_PROCESSING, f"Start retrieving the {index}-th table from {db_type}.")
            logging.info(f"Retrieving the {index}-th table from {db_type}.")

            username = db_connection['UserName'] if 'UserName' in db_connection else ''
            password = db_connection['Password'] if 'Password' in db_connection else ''
            ip = db_connection['Ip'] if 'Ip' in db_connection else ''
            port = db_connection['Port'] if 'Port' in db_connection else ''

            db_name  = db_info['DbName'] if 'DbName' in db_info else ''
            filename = db_info['TableName'] if 'TableName' in db_info else ''
            columns = db_info['ColumnsName'] if 'ColumnsName' in db_info else ''
            table_name = db_info['TableName'] if 'TableName' in db_info else ''
            query = db['SqlStatement']

            header = 0
            tmp_csv_path = f"{tmp_dir}/df{index}_{task_id}_{ts}.csv"

            if db_type == 'mysql' or db_type == 'mssql' or db_type == 'phoenix' or db_type == 'oracle':
                if db_type == 'mysql':
                    db_url   = 'mysql+pymysql://%s:%s@%s:%s/%s' % (username, password, ip, port, db_name)
                elif db_type == 'mssql':
                    db_url   = 'mssql+pymssql://%s:%s@%s:%s/%s' % (username, password, ip, port, db_name)                    
                elif db_type == 'phoenix':
                    db_url   = 'phoenix://%s:%s/' % (ip, port)
                elif db_type == 'oracle':
                    db_url   = 'oracle+cx_oracle://%s:%s@%s:%s' % (username, password, ip, port)

                db_engine = create_engine(db_url).connect().execution_options(stream_results=True)
                header = 0
                for chunk_dataframe in pd.read_sql(query, db_engine, chunksize=10000):
                    if header == 0:
                        if db_type == 'oracle':
                            for column_name in chunk_dataframe.columns:
                                chunk_dataframe =  chunk_dataframe.rename( columns={column_name:column_name.upper()} )
                        chunk_dataframe.to_csv(tmp_csv_path, mode='w', index=False, header=True)
                        header += 1
                    else:
                        chunk_dataframe.to_csv(tmp_csv_path, mode='a', index=False, header=False)
                create_engine(db_url).dispose()
            
            elif db_type == 'cassandra':
                auth_provider = PlainTextAuthProvider(username, password)
                cluster = Cluster([ip],port=int(port),auth_provider=auth_provider)
                session = cluster.connect()
                rows = session.execute(query)
                globals()['df%d'%index] = pd.DataFrame(rows)
                cluster.shutdown()
                globals()['df%d'%index].to_csv(tmp_csv_path, mode='w', index=False, header=True)
                globals()['df%d'%index] = globals()['df%d'%index][0:0]

            elif db_type == 'elasticsearch':
                index_name = db_info['TableName']
                keynames   = db_info['ColumnsName']
                if query == "":
                    filter_js = {}
                else:
                    filter_js  = json.loads(query)

                es = Elasticsearch(hosts=ip, port=port, http_auth=(username, password))
                es_result = helpers.scan(
                    client  = es,
                    query   = filter_js,
                    _source = keynames,
                    index   = index_name,
                    scroll  = '100m',
                    timeout = "100m",
                    size    = 10000
                )

                for k in es_result:
                    val_dict = k['_source']
                    tmp_dict = {}
                    for keyname in keynames:
                        if keyname.find('.') != -1:
                            keys = keyname.split('.')
                            val = val_dict
                            for layer in range(len(keys)):
                                val = val[keys[layer]]
                        else:
                            # The if check is required because some column will be null
                            if keyname in val_dict:
                                val = val_dict[keyname]
                            else:
                                val = ""
                        if type(val) == dict or type(val) == list:
                            val = json.dumps(val)
                        tmp_dict[keyname] = val

                    if header == 0:
                        pd.DataFrame([tmp_dict], columns=keynames).to_csv(tmp_csv_path, mode='w', index=False, header=True)
                        header += 1
                    else:
                        pd.DataFrame([tmp_dict], columns=keynames).to_csv(tmp_csv_path, mode='a', index=False, header=False)                            
                es.transport.close()

            elif db_type == 'mongodb':
                if query == "":
                    filter = {}
                else:
                    filter  = json.loads(query)
                projection = {
                    "_id": 0
                }
                columns = db_info['ColumnsName']
                for column_name in columns:
                    projection[column_name] = 1
                if username != '':
                    mongodb_client = MongoClient(f'mongodb://{username}:{password}@{ip}:{port}/')
                else:
                    mongodb_client = MongoClient(f'mongodb://{ip}:{port}/')
                mongodb_db = mongodb_client[db_name]
                mongodb_cursor = mongodb_db[table_name].find(filter, projection, batch_size=10000)
                globals()['df%d'%index] = pd.DataFrame(list(mongodb_cursor))
                mongodb_client.close()
                tmp_csv_path = f"{tmp_dir}/df{index}_{task_id}_{ts}.csv"
                globals()['df%d'%index].to_csv(tmp_csv_path, mode='w', index=False, header=True)
                globals()['df%d'%index] = globals()['df%d'%index][0:0]

            elif db_type == 'hbase':
                connection = happybase.Connection(ip, port=int(port))
                happybase_table = happybase.Table(table_name, connection)
                b_columns = [str.encode(s) for s in columns]
                data = ()
                if query != "":
                    data = happybase_table.scan(columns = b_columns, filter = query)
                else:
                    data = happybase_table.scan(columns = b_columns)
                my_list = ((list(["" if d.get(col) is None else d[col].decode('utf-8') for col in b_columns])) for _, d in data)
                my_data = pd.DataFrame(my_list, columns=columns)
                globals()['df%d'%index] = my_data
                connection.close()
                globals()['df%d'%index].to_csv(tmp_csv_path, mode='w', index=False, header=True)
                globals()['df%d'%index] = globals()['df%d'%index][0:0]

            elif db_type == 'excel' or db_type == 'csv' or db_type == 'hds':
                if db_type =='excel' or db_type == 'csv':
                    params = {'from': f'hds:///join/upload-{db_type}/{filename}', 'to': 'local:///'}                
                elif db_type == 'hds':
                    params = {'from': f'hds:///join/csv/{filename}', 'to': 'local:///'}

                encoded = urllib.parse.urlencode(params)
                url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}'

                if db_type == 'excel' or db_type == 'hds':
                    my_data = pd.read_excel(url)
                elif db_type == 'csv':
                    my_data = pd.read_csv(url)

                if len(columns) > 0:
                    my_data = my_data[[x for x in columns]]
                globals()['df%d'%index] = my_data
                globals()['df%d'%index].to_csv(tmp_csv_path, mode='w', index=False, header=True)
                globals()['df%d'%index] = globals()['df%d'%index][0:0]

            elif db_type == 'None':
                pass
            else:
                logging.error("Unsupported DB type " + db_type)
                send_task_status(task_id, TASKSTATUS_FAILED, "Unsupported DB type " + db_type)
                exit(1)

            end_retrieve_time = time.time()
            logging.info(f'Finished retrieving the {index}-th table from {db_type}. Time:{str(end_retrieve_time - start_retrieve_time)}')

            remove_hadoop_df_csv(index)
            cmd = f'/opt/hadoop-3.3.4/bin/hadoop fs -copyFromLocal {tmp_csv_path} /join/df{index}_{task_id}_{ts}.csv'
            process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
            logging.debug(f"hadoop fs -copyFromLocal: stdout:{stdout}, stderr:{stderr}")
            os.remove(tmp_csv_path)

        except Exception as e:
            logging.error(f"Error in retrieving {index}-th table from {db_type}: " + traceback.format_exc())
            send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving file: " + str(e))
            exit(1)

''' ========== pull_and_join  ========== '''
def pull_and_join(task_id, task_dict):
    rename_col = dict()
    for merge_idx, merge_info in enumerate(task_dict['Merge']['Tables']):
        start_time = time.time()

        left_idx, right_idx = merge_idx, merge_idx+1
        left_info, right_info = task_dict['Base']['Databases'][left_idx], task_dict['Base']['Databases'][right_idx]

        if 'Condition' in left_info:
            left_function_cond = left_info['Condition']['FunctionCondition']
            left_special_func = left_info['Condition']['SpecialFunction'] if 'SpecialFunction' in left_info else None
        else:
            left_function_cond, left_special_func = None, None

        if 'Condition' in right_info:
            right_function_cond = right_info['Condition']['FunctionCondition']
            right_special_func = right_info['Condition']['SpecialFunction'] if 'SpecialFunction' in right_info else None
        else:
            right_function_cond, right_special_func = None, None

        try:
            if merge_idx == 0:
                globals()['df0'] = sparkSession.read.option("header","true").csv("hdfs:///join/" + f"df{str(left_idx)}_{task_id}_{ts}.csv")
                globals()['df1'] = sparkSession.read.option("header","true").csv("hdfs:///join/" + f"df{str(right_idx)}_{task_id}_{ts}.csv")

                special_function(0, left_special_func)
                datasource_function(0, left_function_cond)
                special_function(1, right_special_func)
                datasource_function(1, right_function_cond)
            else:
                globals()['df1'] = sparkSession.read.option("header","true").csv("hdfs:///join/" + f"df{str(right_idx)}_{task_id}_{ts}.csv")
                special_function(1, right_special_func)
                datasource_function(1, right_function_cond)

        except Exception as e:
            logging.error("Error in setup data: " + traceback.format_exc())
            send_task_status(task_id, TASKSTATUS_FAILED, "Error in setup data: " + str(e))
            exit(1)
        
        end_time = time.time()
        logging.info(f'Finished setup table for task-{merge_idx}. Time:{str(end_time - start_time)}')

        # join
        if len(task_dict['Merge']['Tables']) < 1 or task_dict['Base']['Databases'][right_idx]['ConnectionInfo']['DbType'] == 'None':
            df_joined = globals()['df0']
        else:
            try:
                start_join_time = time.time()
                logging.info('Start joining two tables')
                send_task_status(task_id, TASKSTATUS_PROCESSING, "Start joining two tables")
                #   check if have conflicting column
                conflicting_rename(merge_info, rename_col, task_dict)
                cond = []
                for index_column, left_table_merge_column in enumerate(merge_info[0]['MergeColumnName']):
                    right_table_merge_column = merge_info[1]['MergeColumnName'][index_column]
                    if left_table_merge_column == right_table_merge_column:
                        globals()['df1'] = globals()['df1'].withColumnRenamed(right_table_merge_column, f'{right_table_merge_column}_y')
                        cond.append(  col(left_table_merge_column) == col(right_table_merge_column+"_y")  )
                    else:
                        cond.append(  col(left_table_merge_column) == col(right_table_merge_column)  )

                df_joined = globals()['df0'].join(globals()['df1'], cond , "left") #.persist(StorageLevel.DISK_ONLY)

                # delete the column in the table on the right
                for index_column, left_table_column in enumerate(merge_info[0]['MergeColumnName']):
                    right_table_column = merge_info[1]['MergeColumnName'][index_column]
                    if left_table_column == right_table_column:
                        df_joined = df_joined.drop(f"{right_table_column}_y")
                    else:
                        df_joined = df_joined.drop(f"{right_table_column}")

                send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished joining two tables")
                end_join_time = time.time()
                logging.info(f'Finished joining two tables. Time:{str(end_join_time - start_join_time)}')
            
            except Exception as e:
                logging.error("Error in joining the two tables: " + traceback.format_exc())
                send_task_status(task_id, TASKSTATUS_FAILED, "Error in joining the two tables: " + str(e))
                exit(1)

        globals()['df0'] = df_joined # reuse df0 if it's a pipeline task
    return df_joined


def conflicting_rename(merge_info, rename_col, task_dict):
    #   check if have conflicting column
    merge_columns = []
    columnTypeList = task_dict['ResultParameterSetting']['ColumnTypeList']
    for i in range(2):
        for index_column, column in enumerate(merge_info[i]['MergeColumnName']):
            merge_columns.append(merge_info[i]['MergeColumnName'][index_column])
    # df0_colname = globals()['df0'].columns
    # df1_colname = globals()['df1'].columns
    # logging.info(f'df0_colname : {df0_colname}')
    # logging.info(f'df1_colname : {df1_colname}')
    # logging.info(f'merge_columns : {merge_columns}')
    have_same_colname = set(globals()['df0'].columns) & set(globals()['df1'].columns) - set(
        merge_columns)  # conflicting column except merge column
    have_been_same_colname = set(globals()['df1'].columns) & set(rename_col)
    have_same_colname.update(have_been_same_colname)
    if have_same_colname:
        logging.info("column name conflicting !")
        logging.info("Start rename conflicting column")
        for conflicting_col in have_same_colname:
            logging.info(f'deal with column: {conflicting_col}')
            index = []  # target column index
            rename_count = 0  # count of rename item
            rename_item = []  # rename_item[0] for left, [1] for right table
            # search all columnTypeList find out the conflicting column's index and wheather if it will rename
            for item_index, item in enumerate(columnTypeList):
                if item["Column"] == conflicting_col:
                    index.append(item_index)
                    if item["Rename"]["IsEnable"] == "True":
                        rename_item.append(1)
                        rename_count += 1
                    else:
                        rename_item.append(0)
                if len(rename_item) == 2:
                    break
            # logging.info(f'index: {index}')
            # logging.info(f'rename_item: {rename_item}')
            # logging.info(f'rename_count: {rename_count}')
            # change conflicting column's original column name
            if conflicting_col in rename_col:
                globals()['df1'] = globals()['df1'].withColumnRenamed(conflicting_col,
                                                                      f'{conflicting_col}_{str(rename_col.get(conflicting_col) + 1)}')
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[0]]["Column"] = conflicting_col + "_" + str(
                    rename_col.get(conflicting_col) + 1)
                rename_col[conflicting_col] += 1
                continue
            if rename_count == 1:
                globals()['df%d' % rename_item.index(1)] = globals()['df%d' % rename_item.index(1)].withColumnRenamed(
                    conflicting_col, f'*{conflicting_col}_{str(index[rename_item.index(1)])}')
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[rename_item.index(1)]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[rename_item.index(1)])
            elif rename_count == 2:
                globals()['df0'] = globals()['df0'].withColumnRenamed(conflicting_col,
                                                                      f'*{conflicting_col}_{str(index[0])}')
                globals()['df1'] = globals()['df1'].withColumnRenamed(conflicting_col,
                                                                      f'*{conflicting_col}_{str(index[1])}')
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[0]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[0])
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[1]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[1])
            elif rename_count == 0:
                globals()['df0'] = globals()['df0'].withColumnRenamed(conflicting_col, f'{conflicting_col}_1')
                globals()['df1'] = globals()['df1'].withColumnRenamed(conflicting_col, f'{conflicting_col}_2')
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[0]]["Column"] = conflicting_col + "_1"
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[1]]["Column"] = conflicting_col + "_2"
                if conflicting_col not in rename_col:
                    rename_col[conflicting_col] = 0
                rename_col[conflicting_col] += 2
            # logging.info(f'rename_col: {rename_col}')
        logging.info("Finished rename conflicting column")
        df0_colname = globals()['df0'].columns
        df1_colname = globals()['df1'].columns
        logging.info(f'df0_colname : {df0_colname}')
        logging.info(f'df1_colname : {df1_colname}')
        logging.info(f'merge_columns : {merge_columns}')


# ========== Download task information(.json) from HDS ==========
try:
    download = sys.argv[1]
    task_file = sys.argv[2]
    ts = str(datetime.now().timestamp())  # timestamp
    hds_host = "hbase-regionserver1"
    hds_port = "8000"
    if download == "true":
        url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?from=hds:///join/json/{task_file}&to=local:///'
        response = urlopen(url)
        # Convert bytes to string type and string type to dict
        json_response = response.read().decode('utf-8')
        task_dict = json.loads(json_response)
    else:
        # read local file
        with open(f"../dcfs-tmp-json/{task_file}", 'r') as rf:
            task_dict = json.load(rf)

    task_id = str(task_dict['TaskAction']['TaskId'])
    
    setup_logging('joined_' + task_id + '_' + ts + '.log')
    logging.info('Task started. task_id = ' + task_id)
    logging.info('Task info:\n' + json.dumps(task_dict))
except Exception as e:
    logging.error("Error: Download task information(.json) from HDS:" + traceback.format_exc())
    send_task_status(str(-1), TASKSTATUS_FAILED, str(e))
    exit(1)

# ========== start join ==========
try:
    # Create a join folder in HDFS for tmp file
    os.makedirs(tmp_dir, exist_ok=True)
    cmd = '/opt/hadoop-3.3.4/bin/hadoop fs -mkdir /join'
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    logging.debug(f"/opt/hadoop-3.3.4/bin/hadoop fs -mkdir /join:stdout:{stdout}, stderr:{stderr}")
    # Start pulling data and joining data
    get_join_data(task_id, task_dict)
    df_joined = pull_and_join(task_id, task_dict)
except Exception as e:
    logging.error("join error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, str(e))
    exit(1)

#  ========== create phoenix sql and auto add primary key(autotimestamp) if not have one ==========
try:
    def generate_phoenix_autotimestamp():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    table_name = task_dict['TaskAction']['TableName'].upper()

    # create phoenix sql and check whether to add auto_timestamp column
    phoenix_sql = ''
    columnTypeList = task_dict['ResultParameterSetting']['ColumnTypeList']
    if task_dict['TaskAction']['Phoenix'] == "True":
        primary_key_info = None
        phoenix_sql += f'CREATE TABLE {table_name} ('
        for column_idx, column_info in enumerate(columnTypeList):
            if column_info['IsPrimaryKey'] == "True":
                primary_key_info = column_info
                if column_info['Column'] == "AUTOTIMESTAMP_":
                    df_autotimestamp = None
                    df_row_number = df_joined.count()
                    header = 0
                    while True:
                        if df_row_number > 10000:
                            df_row_number = df_row_number - 10000
                            autotimestamp = [[generate_phoenix_autotimestamp()] for t in range(10000)]
                            if header == 0:
                                header += 1
                                df_autotimestamp = sparkSession.createDataFrame(autotimestamp,["AUTOTIMESTAMP__"])
                            else:
                                df_10000_tmp = sparkSession.createDataFrame(autotimestamp,["AUTOTIMESTAMP__"])
                                df_autotimestamp.union(df_10000_tmp)
                        if df_row_number <= 10000:
                            break
                    autotimestamp = [[generate_phoenix_autotimestamp()] for t in range(df_row_number)]
                    # df_autotimestamp = sparkSession.sparkContext.parallelize(autotimestamp).toDF(['AUTOTIMESTAMP__'])
                    df_10000_tmp = sparkSession.createDataFrame(autotimestamp,["AUTOTIMESTAMP__"])
                    if df_autotimestamp == None :
                        # append the AUTOTIMESTAMP__ column to the df_joined
                        df_joined = df_joined.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                        df_10000_tmp = df_10000_tmp.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                        df_joined = df_joined.join(df_10000_tmp, on=["row_index"]).drop("row_index")
                        df_10000_tmp = None
                    else:
                        df_autotimestamp.union(df_10000_tmp)
                        df_10000_tmp = None
                        df_joined = df_joined.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                        df_autotimestamp = df_autotimestamp.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                        df_joined = df_joined.join(df_autotimestamp, on=["row_index"]).drop("row_index")
                        df_autotimestamp = None
            else:
                rename = column_info['Rename']
                if rename['IsEnable'] == "True": 
                    phoenix_sql += '\"' + rename['NewName'] + '\" ' + column_info['Type'] + ","
                else:
                    phoenix_sql += '\"' + column_info['Column'] + '\" ' + column_info['Type'] + ","
            if len(columnTypeList) -1 == column_idx:
                rename = primary_key_info['Rename']
                if rename['IsEnable'] == "True":
                    phoenix_sql += '\"' + rename['NewName'] + '\" ' + primary_key_info['Type'] + ' not null,'
                    phoenix_sql += ' CONSTRAINT pk PRIMARY KEY (\"' + rename['NewName'] + '\"));'
                else:
                    phoenix_sql += '\"' + primary_key_info['Column'] + '\" ' + primary_key_info['Type'] + ' not null,'
                    phoenix_sql += ' CONSTRAINT pk PRIMARY KEY (\"' + primary_key_info['Column'] + '\"));'

    logging.debug('phoenix sql:' + phoenix_sql)

    # save to csv and phoenix sql
    tmp_csv_path = f'{tmp_dir}/joined_{task_id}_{ts}.csv'
    tmp_sql_path = f'{tmp_dir}/joined_{task_id}_{ts}.sql'
    if phoenix_sql != "" and task_dict['TaskAction']['AppendPhoenix'] != 'True':
        with open(tmp_sql_path, 'w') as wf:
            wf.write(phoenix_sql)
except Exception as e:
    logging.error("error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "tmp phoenix's sql and csv path error:" + str(e))
    exit(1)

# ========== Converts spark dataframe to CSV file ==========
try:
    start_csv_time = time.time()
    logging.info(f'Start dataframe convert to csv.')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start dataframe convert to csv.")
    tmp_target = f"/join/joined_{task_id}_{ts}"
    df_joined.write.csv(tmp_target)
    # df_joined.repartition(1000).write.csv(tmp_target)

    # rename column and write to header.csv
    for column_idx, column_info in enumerate(columnTypeList):
        rename = column_info['Rename']
        if rename['IsEnable'] == "True":
            df_joined = df_joined.withColumnRenamed(column_info['Column'], rename['NewName'])

    # get column name
    header = df_joined.columns

    with open(f'{tmp_dir}/{task_id}_{ts}_header.csv', 'w') as f:
        for index, item in enumerate(header):
            if index == len(header)-1:
                f.write(item+"\n")
            else:
                f.write(item+",")

    # upload hadoop header.csv
    cmd = f'/opt/hadoop-3.3.4/bin/hadoop fs -copyFromLocal {tmp_dir}/{task_id}_{ts}_header.csv /join/joined_{task_id}_{ts}/header.csv'
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    logging.debug(f"hadoop fs -copyFromLocal {tmp_dir}/header.csv /join/joined_{task_id}_{ts}/header.csv: stdout:{stdout}, stderr:{stderr}")

    # merge join csv
    cmd = f'/opt/hadoop-3.3.4/bin/hadoop fs -getmerge -skip-empty-file {tmp_target} {tmp_csv_path}'
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    logging.debug(f"hadoop fs -getmerge -skip-empty-file: stdout:{stdout}, stderr:{stderr}")

    end_csv_time = time.time()
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished dataframe convert to csv.")
    logging.info(f'Finished dataframe convert to csv. Time:{str(end_csv_time - start_csv_time)}')
except Exception as e:
    logging.error("Converts spark dataframe to CSV file error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, str(e))
    exit(1)

# ========== Store data into HDS ==========
try:
    start_hds_time = time.time()
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start importing csv file into HDS")
    logging.info("Start importing csv file into HDS")
    hds_error_message = "Error importing csv file into HDS. Error message:"

    params = {'from': f'local:///', 'to': f'hds:///join/csv/{table_name}.csv'}
    encoded = urllib.parse.urlencode(params)
    cmd = f'curl -T "{tmp_csv_path}" -L -X POST "http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}"'

    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    stdout, stderr = process.communicate()
    exit_code = process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')

    stdout_json = json.loads(stdout)
    hds_error = stdout_json['task']['state']
    if hds_error != 'SUCCEED':
        logging.error(f"{hds_error_message} stdout:{stdout}, stderr:{stderr}")
        send_task_status(task_id, TASKSTATUS_ERROR, f"{hds_error_message} stdout:{stdout}, stderr:{stderr}")
        exit(1)
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished importing csv file into HDS")
    end_hds_time = time.time()
    logging.info(f"Finished importing csv file into HDS. Time:{str(end_hds_time-start_hds_time)}")
except Exception as e:
    logging.error(f"{hds_error_message} e:{traceback.format_exc()}, stdout:{stdout}, stderr:{stderr}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"{hds_error_message} e:{str(e)}, stdout:{stdout}, stderr:{stderr}")
    exit(1)

# ========== Store table into Phoenix ==========
try:
    if task_dict['TaskAction']['Phoenix'] == 'True':
        phoenix_error_message = "Error in when importing table into Phoenix:"
        phoenix_home = "/opt/phoenix-hbase-2.4-5.1.2-bin"
        hds_ip = 'zoo1'
        start_phoenix_time = time.time()
        logging.info('Start importing table into Phoenix')
        send_task_status(task_id, TASKSTATUS_PROCESSING, "Start importing table into Phoenix")

        cmd = phoenix_home + "/bin/psql.py -h in-line %s -t \"%s\" %s %s" % (hds_ip, table_name, tmp_sql_path, tmp_csv_path)
        process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        stdout, stderr = process.communicate()
        exit_code = process.wait()
        stdout = stdout.decode('utf-8')
        stderr = stderr.decode('utf-8')

        end_phoenix_time = time.time()
        logging.info(f'Finished importing table into Phoenix. Time:{end_phoenix_time - start_phoenix_time}')
        send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished importing table into Phoenix")

        logging.debug("Phoenix stdout:" + stdout)
        logging.debug("Phoenix stderr:" + stderr)
        logging.debug(f"Phoenix exit code: {exit_code}")
        if exit_code != 0:
            logging.error(f"{phoenix_error_message}" + stderr)
            send_task_status(task_id, TASKSTATUS_FAILED, f"{phoenix_error_message}" + stderr)
            exit(1)
        elif stderr.find("ERROR") != -1:
            logging.error(f"{phoenix_error_message}" + stderr)
            send_task_status(task_id, TASKSTATUS_FAILED, f"{phoenix_error_message}" + stderr)
            exit(1)
except Exception as e:
    logging.error("Error in when importing table into Phoenix:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "Error in when importing table into Phoenix:" + str(e))
    exit(1)


'''========== Metadata stats =========='''
check_metadata_err = False
metadata_err_arr = []
def process_data(data):
    databases = data['Base']['Databases']

    column_count = defaultdict(int)
    for index, db in enumerate(databases):
        db_name = db['DbInfo']['DbName']
        table_name = db['DbInfo']['TableName']
        columns_name = db['DbInfo']['ColumnsName']
        connection_info = db['ConnectionInfo']
        connection_info_db_type = connection_info['DbType']
        if connection_info_db_type == 'None':
            continue
        connection_info_ip = connection_info['Ip']
        connection_info_port = connection_info['Port']
        connection_info_user_name = connection_info['UserName']
        for column in columns_name:
            column_count[(connection_info_db_type, connection_info_ip, connection_info_port, connection_info_user_name, db_name, table_name, column, index)] += 1

    return column_count


##join key
def process_merge_columns(count_task_dict):
    merge_column_count = defaultdict(int)
    merge_tables = count_task_dict["Merge"]["Tables"]
    for index, merge_table in enumerate(merge_tables):
        for i, merge_column_info in enumerate(merge_table):
            db_info = count_task_dict["Base"]["Databases"][index+i]["DbInfo"]
            db_name = db_info["DbName"]
            table_name = db_info["TableName"]

            for column_name in merge_column_info["MergeColumnName"]:
                merge_column_count[(db_name, table_name, column_name, index+i)] += 1
    #reduce repeated calculation's mergecount           
    for key, value in merge_column_count.items():
        key_prefix = key[:3]
        prev_key = key[:3] + (key[3] - 1,)
        if prev_key in merge_column_count:
            merge_column_count[prev_key] -=1
    logging.debug(f"merge_column_count = {merge_column_count}")
    return merge_column_count

def generate_autotimestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def cal_function_times(data, col_name):
    func = data['Condition']['FunctionCondition']
    cnt = {}

    for k, v in func.items():
        cnt[k] = 0
        for item in v:
            if item["IsEnable"] == "False":
                continue
            if k in ["ToSum", "ToAvg", "ToMode"] and item["TargetColumn"] == col_name:
                    cnt[k] += 1
            elif k == "ToColumnCombine":
                for j in item["TargetColumnList"]:
                    if j["TargetColumn"] == col_name:
                        cnt[k] += 1
            elif k == "ToPivot" and (item["TargetColumn"] == col_name or col_name in item["TargetValues"]):
                    cnt[k] += 1
            elif k == "ToArithmetic":
                for j in item["TargetColumnList"]:
                    if j["Column"] == col_name:
                        cnt[k] += 1
    return cnt

def upsert_counts_to_phoenix(task_dict, Count_table_name, conn):
    try:
        count_task_dict = task_dict
        #collect metadata stats
        SubmittedUser = count_task_dict['TaskAction']['SubmittedUser']
        column_counts = process_data(count_task_dict)
        merge_column_count = process_merge_columns(count_task_dict)
        
        create_table_query = f'CREATE TABLE IF NOT EXISTS {Count_table_name} (id varchar primary key,"ConnectionInfo_DbType" varchar, "ConnectionInfo_Ip" varchar, "ConnectionInfo_Port" varchar, "ConnectionInfo_UserName" varchar, "Db_name" varchar, "Table_name" varchar, "Column_name" varchar, "Column_count" bigint, "Column_Frequency" varchar,"Last_Used_Column" VARCHAR, "SubmittedUser" VARCHAR,"Merge_Column_Count" bigint, "Function_Sum_Count" tinyint, "Function_Avg_Count" tinyint, "Function_Mode_Count" tinyint, "Function_ColCombine_Count" tinyint, "Function_Pivot_Count" tinyint, "Function_Arithmetic_Count" tinyint)'
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        #upsert into phoenix by each column
        for (connection_info_db_type, connection_info_ip, connection_info_port, connection_info_user_name,db_name, table_name, column_name, index), count in column_counts.items():
            logging.debug(f"Processing column: {column_name} with count: {count}")
            unique_id = f"{connection_info_ip}:{connection_info_port}:{db_name}:{table_name}:{column_name}"
            select_query = f'SELECT "ID", "Column_count", "Last_Used_Column", "SubmittedUser","Merge_Column_Count", "Function_Sum_Count", "Function_Avg_Count", "Function_Mode_Count", "Function_ColCombine_Count", "Function_Pivot_Count", "Function_Arithmetic_Count" FROM {Count_table_name} WHERE "ID" = ?'
            cursor.execute(select_query, (unique_id,))
            row = cursor.fetchone()
            function_count = cal_function_times(count_task_dict['Base']['Databases'][index], column_name)

            if row is not None:
                current_id, current_count, last_used_column, current_users, current_merge_column_count, sum_cnt, avg_cnt,mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt  = row
                # add user
                users_list = current_users.split(',')
                if SubmittedUser not in users_list:
                    users_list.append(SubmittedUser)
                updated_users = ','.join(users_list)

                updated_merge_cnt = merge_column_count.get((db_name, table_name, column_name, index), 0) + current_merge_column_count
                updated_count = current_count + count
                current_time = generate_autotimestamp()
                last_used_datetime = datetime.strptime(last_used_column, "%Y-%m-%d %H:%M:%S")
                current_time_datetime = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
                time_diff = str((current_time_datetime - last_used_datetime))

                sum_cnt += function_count["ToSum"]
                avg_cnt += function_count["ToAvg"]
                mode_cnt += function_count["ToMode"]
                col_combine_cnt += function_count["ToColumnCombine"]
                pivot_cnt += function_count["ToPivot"]
                arithmetic_cnt += function_count["ToArithmetic"]

                update_query = f'UPSERT INTO {Count_table_name} ("ID", "Column_name", "Column_count", "ConnectionInfo_DbType", "ConnectionInfo_Ip", "ConnectionInfo_Port", "ConnectionInfo_UserName", "Db_name", "Table_name", "Last_Used_Column", "Column_Frequency", "SubmittedUser", "Merge_Column_Count", "Function_Sum_Count", "Function_Avg_Count", "Function_Mode_Count", "Function_ColCombine_Count", "Function_Pivot_Count", "Function_Arithmetic_Count") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                logging.debug(f"Parameters for update query: unique_id={unique_id}, column_name={column_name}, updated_count={updated_count}, connection_info_db_type={connection_info_db_type}, connection_info_ip={connection_info_ip}, connection_info_port={connection_info_port}, connection_info_user_name={connection_info_user_name}, db_name={db_name}, table_name={table_name}, current_time={current_time}, time_diff={time_diff}, updated_users={updated_users}, updated_merge_cnt={updated_merge_cnt}, function_count={(sum_cnt, avg_cnt, mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt)}")

                cursor.execute(update_query, (unique_id, column_name, updated_count, connection_info_db_type, connection_info_ip, connection_info_port, connection_info_user_name, db_name, table_name, current_time, time_diff, updated_users, updated_merge_cnt, sum_cnt, avg_cnt, mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt))

            else:
                insert_query = f'UPSERT INTO {Count_table_name} ("ID", "Column_name", "Column_count", "ConnectionInfo_DbType", "ConnectionInfo_Ip", "ConnectionInfo_Port", "ConnectionInfo_UserName", "Db_name", "Table_name", "Last_Used_Column", "Column_Frequency", "SubmittedUser", "Merge_Column_Count", "Function_Sum_Count", "Function_Avg_Count", "Function_Mode_Count", "Function_ColCombine_Count", "Function_Pivot_Count", "Function_Arithmetic_Count") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                updated_merge_cnt = merge_column_count.get((db_name, table_name, column_name, index), 0)
                sum_cnt, avg_cnt, mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt = function_count["ToSum"], function_count["ToAvg"], function_count["ToMode"], function_count["ToColumnCombine"], function_count["ToPivot"], function_count["ToArithmetic"]

                current_time = generate_autotimestamp()
                initial_time = "00:00:00"
                logging.debug(f"Parameters for insert query: unique_id={unique_id}, column_name={column_name}, count={count}, connection_info_db_type={connection_info_db_type}, connection_info_ip={connection_info_ip}, connection_info_port={connection_info_port}, connection_info_user_name={connection_info_user_name}, db_name={db_name}, table_name={table_name}, current_time={current_time}, initial_time={initial_time}, SubmittedUser={SubmittedUser}, updated_merge_cnt={updated_merge_cnt}, function_count={(sum_cnt, avg_cnt, mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt)}")

                cursor.execute(insert_query, (unique_id, column_name, count, connection_info_db_type, connection_info_ip, connection_info_port, connection_info_user_name, db_name, table_name, current_time, initial_time, SubmittedUser, updated_merge_cnt, sum_cnt, avg_cnt, mode_cnt, col_combine_cnt, pivot_cnt, arithmetic_cnt))

            conn.commit()

    except Exception as e:
        logging.error("Error in upsert metadata into phoenix: " + traceback.format_exc())
        send_task_status(task_id, TASKSTATUS_FAILED, "Error in upsert metadata into phoenix: " + str(e))
        check_metadata_err = True
        metadata_err_arr.append(e)

try:
    logging.info(f'Start to Metadata stats')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start to store metadata stats.")
    phoenix_ip = "hbase-master1"
    conn = phoenixdb.connect(f'http://{phoenix_ip}:8765/', autocommit=True)
    cursor = conn.cursor()
    #This table_name meeting influence after Neo4j's reading
    Count_table_name = 'METADATA_STATS'    
    upsert_counts_to_phoenix(task_dict, Count_table_name, conn)

except Exception as e:
    logging.error(f"An error occurred when update column counts into phoenix: {str(e)}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"Error in update Metadata stats into phoenix table: {str(e)}")
conn.close()

'''========== creat node to neo4j =========='''
class Neo4jRelated:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()

    def create_nodes_and_relationships(self, columns, column_stats):
        with self.driver.session() as session:
            # Create nodes for all tables
            tables = list(set([(c[4], c[2], c[3]) for c in columns]))  # Extract unique (tablename, ip, port) tuples from columns

            for table_name, ip, port in tables:
                session.write_transaction(self._create_table_node, table_name, ip, port)

            # Create nodes for all columns
            for column in columns:

                stats = next((s for s in column_stats if s['Column_name'] == column[0] and s['Db_name'] == column[5] and s['Table_name'] == column[4]), None)

                logging.debug("Processing column: {}".format(str(column).replace('\n', ' ')))
                session.write_transaction(self._create_node, column, stats)

            # Create relationships for columns 系統說會出現笛卡爾積
            for i, c1 in enumerate(columns):
                for c2 in columns[i+1:]:
                    query = f"""
                        MATCH (a:Column {{name: '{c1[0]}', dbtype: '{c1[1]}', ip: '{c1[2]}', port: '{c1[3]}', tablename: '{c1[4]}', dbname: '{c1[5]}'}}),
                        (b:Column {{name: '{c2[0]}', dbtype: '{c2[1]}', ip: '{c2[2]}', port: '{c2[3]}', tablename: '{c2[4]}', dbname: '{c2[5]}'}})
                        MERGE (a)-[r:RELATED]->(b)
                        MERGE (b)-[r2:RELATED]->(a)
                    """
                    session.run(query)
            # Create relationships for columns 尚未測試
            # for i, c1 in enumerate(columns):
            #     for c2 in columns[i+1:]:
            #         query = f"""
            #             MATCH (a:Column {{name: '{c1[0]}', dbtype: '{c1[1]}', ip: '{c1[2]}', port: '{c1[3]}', tablename: '{c1[4]}', dbname: '{c1[5]}'}})
            #             OPTIONAL MATCH (a)-[:RELATED]-(b:Column {{name: '{c2[0]}', dbtype: '{c2[1]}', ip: '{c2[2]}', port: '{c2[3]}', tablename: '{c2[4]}', dbname: '{c2[5]}'}})
            #             MERGE (a)-[r:RELATED]->(b)
            #             MERGE (b)-[r2:RELATED]->(a)
            #         """
            #         session.run(query)

            # Create relationships for tables
            for i, t1 in enumerate(tables):
                for t2 in tables[i+1:]:
                    query = f"""
                        MATCH (a:Table {{tablename: '{t1[1]}', ip: '{columns[0][2]}', port: '{columns[0][3]}'}}),
                        (b:Table {{tablename: '{t2[1]}', ip: '{columns[0][2]}', port: '{columns[0][3]}'}})
                        MERGE (a)-[r:TABLE_RELATED]->(b)
                        MERGE (b)-[r2:TABLE_RELATED]->(a)
                    """
                    session.run(query)

            # Create relationships from tables to columns
            for column in columns:
                query = f"""
                    MATCH (t:Table {{tablename: '{column[4]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (c:Column {{name: '{column[0]}', dbtype: '{column[1]}', ip: '{column[2]}', port: '{column[3]}', tablename: '{column[4]}', dbname: '{column[5]}'}})
                    MERGE (t)-[r:HAS_COLUMN]->(c)
                """
                session.run(query)
            
            #Create relationships for dbnames
            dbnames = list(set([(c[5], c[2], c[3]) for c in columns]))  # Extract unique (dbname, ip, port) tuples from columns

            for db_name, ip, port in dbnames:
                session.write_transaction(self._create_dbname_node, db_name, ip, port)
           # Create relationships from dbname to tables
            for column in columns:
                query = f"""
                    MATCH (d:Database {{dbname: '{column[5]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (t:Table {{tablename: '{column[4]}', ip: '{column[2]}', port: '{column[3]}'}})
                    MERGE (d)-[r:HAS_TABLE]->(t)
                """
                session.run(query)
            
            #Create relationships for dbtypes
            dbtype_ip_port_combinations = list(set([(c[1], c[2], c[3]) for c in columns]))  # Extract unique (dbtype, ip, port) tuples from columns

            for dbtype, ip, port in dbtype_ip_port_combinations:
                session.write_transaction(self._create_dbtype_node, dbtype, ip, port)

            # Create relationships from DBType to Database
            for column in columns:
                query = f"""
                    MATCH (t:DBType {{dbtype: '{column[1]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (d:Database {{dbname: '{column[5]}', ip: '{column[2]}', port: '{column[3]}'}})
                    MERGE (t)-[r:HAS_DATABASE]->(d)
                """
                session.run(query)

    #update unique node
    def create_unique_constraint(self):
        with self.driver.session() as session:
            query = """
                CREATE CONSTRAINT unique_column IF NOT EXISTS
                FOR (c:Column)
                REQUIRE (c.ip, c.port, c.dbname, c.dbtype, c.tablename, c.name) IS UNIQUE
            """
            session.run(query)
    
    def get_related_columns(self):
        with self.driver.session() as session:
            query = """
                MATCH (c1:Column)-[:RELATED]->(c2:Column)
                RETURN c1.phoenix_ID, collect(c2.phoenix_ID) AS related_columns
        """

            result = session.run(query)
            related_columns_data = {}

            for record in result:
                doc_id = f"{record['c1.phoenix_ID']}"
                related_columns = record['related_columns']
                related_columns_data[doc_id] = related_columns

        return related_columns_data

    @staticmethod
    def _create_node(tx, column, stats):
        query = """
            MERGE (c:Column {
                name: $name,
                dbtype: $dbtype,
                ip: $ip,
                port: $port,
                tablename: $tablename,
                dbname: $dbname
            })
        """
        parameters = {
            'name': column[0],
            'dbtype': column[1],
            'ip': column[2],
            'port': column[3],
            'tablename': column[4],
            'dbname': column[5]
        }
        #phoenix stats set column to neo4j
        if stats:
            query += """
                SET c.column_count = $column_count,
                c.column_frequency = $column_frequency,
                c.phoenix_ID = $phoenix_ID,
                c.last_used_column = $last_used_column,
                c.merge_column_count = $merge_column_count,
                c.SubmittedUser = $SubmittedUser
            """
            parameters.update({
                'column_count': stats['Column_count'],
                'column_frequency': stats['Column_Frequency'],
                'phoenix_ID': stats['ID'],
                'last_used_column': stats['Last_Used_Column'],
                'merge_column_count': stats['Merge_Column_Count'],
                'SubmittedUser': stats['SubmittedUser'].split(",")
            })

            logging.debug(f"Found stats for column: {column[0]} with DbName: {column[5]}")
        else:
            logging.warning(f"No stats found for column: {column[0]} with DbName: {column[5]}")

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated node: {record['c']}")

    @staticmethod
    def _create_table_node(tx,table_name, ip, port):
        query = """
            MERGE (t:Table {
                tablename: $tablename,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'tablename': table_name,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated table node: {record['t']}")

    @staticmethod
    def _create_dbname_node(tx, db_name, ip, port):
        query = """
            MERGE (d:Database {
                dbname: $dbname,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'dbname': db_name,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated database node: {record['d']}")

    @staticmethod
    def _create_dbtype_node(tx, dbtype, ip, port):
        query = """
            MERGE (d:DBType {
                dbtype: $dbtype,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'dbtype': dbtype,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated DBType node: {record['d']}")

logging.info(f'Neo4j start.')
send_task_status(task_id, TASKSTATUS_PROCESSING, "Neo4j start.")
def get_column_stats_from_phoenix(phoenix_ip, Count_table_name):
    conn = phoenixdb.connect(f'http://{phoenix_ip}:8765/', autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {Count_table_name}')
    columns = [desc[0] for desc in cursor.description]
    column_stats = [dict(zip(columns, row)) for row in cursor.fetchall()]
    #column_stats = [{'Column_name': row[0], 'Db_Name': row[1], 'Table_Name': row[2], 'Column_count': row[3], 'Column_Frequency': row[4], 'Join_Count': row[5], 'Last_Used_Column': row[6]} for row in cursor.fetchall()]

    logging.debug(f"Column stats: {column_stats}")
    conn.close()
    return column_stats

try:
    logging.info(f'Start to create node to neo4j.')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start to create node to neo4j.")

    columns = []
    for db in task_dict['Base']['Databases']:
        if db['ConnectionInfo']['DbType'] == 'None':
            continue
        for col in db['DbInfo']['ColumnsName']:
            columns.append((col, db['ConnectionInfo']['DbType'], db['ConnectionInfo']['Ip'], db['ConnectionInfo']['Port'], db['DbInfo']['TableName'], db['DbInfo']['DbName']))
    #columns.sort(key=lambda x: (x[5], x[4], x[0]))  # Sort by DbName, TableName, ColumnName
    logging.debug(f"Columns: {columns}")
    column_stats = get_column_stats_from_phoenix(phoenix_ip, Count_table_name)
    neo4j_uri = "bolt://neo4j:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    # Initialize Neo4jRelated instance
    neo4j_conn = Neo4jRelated(neo4j_uri, neo4j_user, neo4j_password)
    # Create unique constraint
    neo4j_conn.create_unique_constraint()
    # Create nodes and relationships
    neo4j_conn.create_nodes_and_relationships(columns, column_stats)
    neo4j_conn.close()

except Exception as e:
    logging.error(f"An error occurred when creating node in Neo4j: {str(e)}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"Error in Neo4j processing: {str(e)}")
    check_metadata_err = True
    metadata_err_arr.append(e)

''' ========== json transfer to ELK =========='''
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
        logging.error(f"error when get metadata from {DbType}:" + traceback.format_exc())
        #send_task_status(task_id, TASKSTATUS_FAILED, f"error when git metadata from {DbType}:" + str(e))

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
        logging.error(f"Failed to index document(s): {e}")

    es.close()

metadata_list = []
logging.info("Start to create metadata list.")
for database in task_dict['Base']['Databases']:
    if database['ConnectionInfo']['DbType'] == 'None':
        continue
    metadata = get_column_names(database)
    metadata_list.append(metadata)

try:
    store_metadata_to_elasticsearch(metadata_list)
except Exception as e:
    check_metadata_err = True
    metadata_err_arr.append(e)

'''========== sync neo4j to ELK =========='''
# Neo4j
neo4j_uri = "bolt://neo4j:7687"
neo4j_user = "neo4j"
neo4j_password = "password"

# Elasticsearch
es_host = "elasticsearch"
es_port = 9200
es_user = "elastic"
es_password = "elastic"

neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
es_client = Elasticsearch([{"host": es_host, "port": es_port}], http_auth=(es_user, es_password))

# Index name (in Elasticsearch)
# Pass the metadata filter to ELK
index_name = "sync_data"
#neo4j_instance = Neo4jRelated(uri, user, password)
neo4j_instance = Neo4jRelated(neo4j_uri, neo4j_user, neo4j_password)
def sync_neo4j_to_elk(task_id):
    try:
        logging.info(f'Start sync neo4j data to elk')
        # Get related_columns data from Neo4j
        related_columns_data = neo4j_instance.get_related_columns()

        # Query Neo4j data
        with neo4j_driver.session() as session:
            #nodes = session.run("MATCH (n) RETURN n")
            nodes = session.run("MATCH (n) WHERE EXISTS(n.phoenix_ID) RETURN n")
            # Query results and sync data to Elasticsearch
            for node in nodes:
                node_properties = dict(node["n"])

                # Filter required attributes
                filtered_properties = {
                    "dbname": node_properties.get("dbname"),
                    "dbtype": node_properties.get("dbtype"),
                    "name": node_properties.get("name"),
                    "tablename": node_properties.get("tablename"),
                    "pagerank": node_properties.get("pagerank"),
                    "usagecount":node_properties.get("column_count"),
                }

                doc_id = node_properties["phoenix_ID"]  #use node name as document id
                related_columns = related_columns_data.get(doc_id, [])
                filtered_properties["related_columns"] = related_columns

                es_client.index(index=index_name, id=doc_id, document=filtered_properties)
        logging.info(f'Finish sync neo4j data to elk')
    except Exception as e:
        logging.error(f"An error occurred when sync neo4j to elk: {str(e)}")
        send_task_status(task_id, "TASKSTATUS_FAILED", f"Error in ELK processing: {str(e)}")
        check_metadata_err = True
        metadata_err_arr.append(e)

# sync
sync_neo4j_to_elk(task_id)


# ========== remove tmp file ==========
def remove_tmp_file(task_dict):
    try:
        os.remove(f'{tmp_dir}/joined_{task_id}_{ts}.csv') if os.path.exists(f'{tmp_dir}/joined_{task_id}_{ts}.csv') else None
        os.remove(f'{tmp_dir}/joined_{task_id}_{ts}.sql') if os.path.exists(f'{tmp_dir}/joined_{task_id}_{ts}.sql') else None
        os.remove(f'{tmp_dir}/{task_id}_{ts}_header.csv') if os.path.exists(f'{tmp_dir}/{task_id}_{ts}_header.csv') else None

        for ind in range(len(task_dict['Base']['Databases'])):
            process = subprocess.Popen("/opt/hadoop-3.3.4/bin/hadoop fs -rm -skipTrash /join/" + f"df{ind}_{task_id}_{ts}.csv", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
            logging.debug(f"hadoop remove tmp df{ind} file. stdout:{stdout}, stderr:{stderr}")

        # remove join tmp dir
        process = subprocess.Popen(f"/opt/hadoop-3.3.4/bin/hadoop fs -rm -r -f {tmp_target}", shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        process.wait()
        stdout = stdout.decode('utf-8')
        stderr = stderr.decode('utf-8')
        logging.debug(f"hadoop remove tmp dir. stdout:{stdout}, stderr:{stderr}")
    except Exception as e:
        logging.error("remove tmp file error:" + traceback.format_exc())
        send_task_status(task_id, TASKSTATUS_ERROR, "remove tmp file error:" + str(e))
        exit(1)

remove_tmp_file(task_dict)

# ========== Job finished ==========
try:
    params = {'from': f'hds:///join/csv/{table_name}.csv', 'to': f'local:///{table_name}.csv', 'redirectfrom':'NULL'}
    encoded = urllib.parse.urlencode(params)
    if check_metadata_err:
        send_task_status(task_id, TASKSTATUS_SUCCEEDED, f"Task finished, but there's something wrong when storing metadata.\n {str(metadata_err_arr)}", f'/dataservice/v1/access?{encoded}', table_name)
        logging.info("Job finished, but there's something wrong when storing metadata.")
    else:
        send_task_status(task_id, TASKSTATUS_SUCCEEDED, "Job finished.", f'/dataservice/v1/access?{encoded}', table_name)
        logging.info("Job finished.")

    end = time.time()
    logging.info("Total time:" + str(end - start) + "(sec)")

    sparkSession.stop()
except Exception as e:
    logging.error("Job finished error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_ERROR, "Job finished error:" + str(e))
    exit(1)

exit()
