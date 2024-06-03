import os
import sys
import logging
import traceback
from datetime import datetime
from func_timeout import func_timeout, FunctionTimedOut
# add paths, otherwise python3 launched by DCFS would go wrong
for p in ['', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload', '/usr/local/lib/python3.10/dist-packages', '/usr/lib/python3/dist-packages']:
    sys.path.append(p)

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
from metadata import get_column_names, store_metadata_to_elasticsearch
from neo4j_related import Neo4jRelated
from metadata_stats import upsert_counts_to_phoenix
from neo4j import GraphDatabase
import phoenixdb
import phoenixdb.cursor
from lineage_tracking import lineage_info, lineage_tracing

start = time.time()

rmq_host = "rabbitmq"
hds_host = "hbase-regionserver1"
hds_port = "8000"
tmp_dir = "/tmp/dcfs"
log_dir = "/dcfs-share/task-logs/"
settint_path = "/dcfs-share/dcfs-run/setting.json"

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
        pika.ConnectionParameters(host=rmq_host, credentials=credentials))
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

''' ========== setting.json ========== '''
with open(settint_path, 'r') as rf:
    try:
        setting = json.load(rf)
        rmq_host = setting['rabbitmq_host']
        hds_host = setting['HDS']['host']
        hds_port = setting['HDS']['port']
    except Exception as e:
        logging.error("Error in setting.json: " + traceback.format_exc())
        send_task_status(str(-1), TASKSTATUS_UNKNOWN, traceback.format_exc())
        exit(1)

''' ========== datasource function  ========== '''
def sum_function(df_index, function_info):
    if "GroupBy" in function_info and function_info["GroupBy"]["IsEnable"] == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        sum_column = function_info["TargetColumn"]
        tmp_group_df = globals()[f'df{df_index}'].groupby(groupBy_column)[sum_column].sum()
        tmp_group_df = pd.DataFrame({groupBy_column: tmp_group_df.index, function_info["Naming"]: tmp_group_df.values})
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].merge(tmp_group_df, how='left', left_on=groupBy_column, right_on=groupBy_column)
        tmp_group_df = tmp_group_df[0:0]
    else:
        # Use numpy to calculate the sum
        result_sum = np.nansum(globals()[f'df{df_index}'][function_info["TargetColumn"]])
        globals()[f'df{df_index}'][function_info["Naming"]] = result_sum

def avg_function(df_index, function_info):
    if "GroupBy" in function_info and function_info["GroupBy"]["IsEnable"] == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        avg_column = function_info["TargetColumn"]
        tmp_group_df = globals()[f'df{df_index}'].groupby(groupBy_column)[avg_column].mean()
        tmp_group_df = pd.DataFrame({groupBy_column: tmp_group_df.index, function_info["Naming"]: tmp_group_df.values})
        globals()[f'df{df_index}'] = globals()[f'df{df_index}'].merge(tmp_group_df, how='left', left_on=groupBy_column, right_on=groupBy_column)
        tmp_group_df = tmp_group_df[0:0]
    else:
        globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["TargetColumn"]].mean()

def mode_function(df_index, function_info):
    globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'].mode()[function_info["TargetColumn"]][0]

def columncombine_function(df_index, function_info):
    # sort TargetColumnList order
    targetColumnList = function_info['TargetColumnList']
    def column_order(elem):
        return elem["Order"]
    targetColumnList.sort(key=column_order)
    for combine_idx, combine_order in enumerate(targetColumnList):
        if combine_idx == 0:
            globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][combine_order["TargetColumn"]].map(str).replace('nan', '')
        else:
            globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["Naming"]] + function_info["Separator"] + globals()[f'df{df_index}'][combine_order["TargetColumn"]].map(str).replace('nan', '')

def pivot_function(df_index, function_info):
    targetColumn = function_info["TargetColumn"]
    aggregateColumn = function_info["AggregateColumn"][0]["Column"]
    aggregateFunction = function_info["AggregateFunction"]
    isGroupBy = function_info["GroupBy"]["IsEnable"]
    if isGroupBy == "True":
        groupBy_column = function_info["GroupBy"]["TargetColumnList"][0]
        if aggregateFunction == "MAX":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupby([groupBy_column, targetColumn])[aggregateColumn].max().unstack(targetColumn).reset_index()
        elif aggregateFunction == "SUM":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupby([groupBy_column, targetColumn])[aggregateColumn].sum().unstack(targetColumn).reset_index()
        elif aggregateFunction == "AVG":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].groupby([groupBy_column, targetColumn])[aggregateColumn].mean().unstack(targetColumn).reset_index()
    else:
        if aggregateFunction == "MAX":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].pivot_table(values=aggregateColumn, columns=targetColumn, aggfunc=np.max)
        elif aggregateFunction == "SUM":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].pivot_table(values=aggregateColumn, columns=targetColumn, aggfunc=np.sum)
        elif aggregateFunction == "AVG":
            globals()[f'df{df_index}'] = globals()[f'df{df_index}'].pivot_table(values=aggregateColumn, columns=targetColumn, aggfunc=np.mean)

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
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
            else:
                globals()[f'df{df_index}'][arithmetic_order["Column"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["Naming"]] + globals()[f'df{df_index}'][arithmetic_order["Column"]]
    
    if operator == "SUB":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
            else:
                globals()[f'df{df_index}'][arithmetic_order["Column"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["Naming"]] - globals()[f'df{df_index}'][arithmetic_order["Column"]]
    
    if operator == "MUL":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
            else:
                globals()[f'df{df_index}'][arithmetic_order["Column"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["Naming"]] * globals()[f'df{df_index}'][arithmetic_order["Column"]]
    
    if operator == "DIV":
        for arithmetic_idx, arithmetic_order in enumerate(targetColumnList):
            if arithmetic_idx == 0:
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
            else:
                globals()[f'df{df_index}'][arithmetic_order["Column"]] = globals()[f'df{df_index}'][arithmetic_order["Column"]].astype(float)
                globals()[f'df{df_index}'][function_info["Naming"]] = globals()[f'df{df_index}'][function_info["Naming"]] / globals()[f'df{df_index}'][arithmetic_order["Column"]]

def datasource_function(df_index, functionCondition, pull_action_RunID):
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
        lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
        lineage_info["Dataset"]["Input"]["Processinfo"].append(pull_action_RunID)
        if execute_function[0] == toSum:
            sum_function(df_index, execute_function[1])
            lineage_info_Action("DF40-1","CALCULATE", "DCFS", "Sum", "Sum")
        if execute_function[0] == toAvg:
            avg_function(df_index, execute_function[1])
            lineage_info_Action("DF40-2","CALCULATE", "DCFS", "Avg", "Avg")
        if execute_function[0] == toMode:
            mode_function(df_index, execute_function[1])
            lineage_info_Action("DF40-3","CALCULATE", "DCFS", "Mode", "Mode")
        if execute_function[0] == toColumnCombine:
            columncombine_function(df_index, execute_function[1])
            lineage_info_Action("DF50-1","CONVERT", "DCFS", "ColumnCombine", "ColumnCombine")
        if execute_function[0] == toPivot:
            pivot_function(df_index, execute_function[1])
            lineage_info_Action("DF40-4","CALCULATE", "DCFS", "Pivot", "Pivot")
        if execute_function[0] == toArithmetic:
            arithmetic_function(df_index, execute_function[1])
            lineage_info_Action("DF40-5","CALCULATE", "DCFS", "Arithmetic", "Arithmetic")
        lineage_info_Run("Success", "null")
        pull_action_RunID = str(lineage_tracing(lineage_info))
        clear_lineage_info()
    end_column_operarion_time = time.time()
    logging.info(f"Finished column operarion time. Time:{str(end_column_operarion_time - start_column_operarion_time)}")
    return pull_action_RunID

''' ========== special function  ========== '''
def split_merge_function(df_index, function_info):
    newLot = function_info['TargetColumn']['NewLot']
    oldLot = function_info['TargetColumn']['OldLot']
    # Get distinct values of the 'NewLot' column
    distinct_newlots = globals()[f'df{df_index}'].drop_duplicates(subset=[newLot], keep='first', inplace=False)
    distinct_newlots.loc[:, oldLot] = distinct_newlots[newLot]
    globals()[f'df{df_index}'] = globals()[f'df{df_index}'].append(distinct_newlots)
    distinct_newlots = distinct_newlots[0:0]

def special_function(df_index, special_function, pull_action_RunID):
    # take Step element for sort
    if special_function is None:
        return pull_action_RunID
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
    lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
    lineage_info["Dataset"]["Input"]["Processinfo"].append(pull_action_RunID)
    lineage_info_Action("DF100-1","SPECIFIC", "DCFS", "SplitMerge", "SplitMerge")
    for execute_function in execute_order:
        if execute_function[0] == splitMerge:
            split_merge_function(df_index, execute_function[1])
    end_special_column_operarion_time = time.time()
    lineage_info_Run("Success", "null")
    pull_action_RunID = str(lineage_tracing(lineage_info))
    clear_lineage_info()
    logging.info(f"Finished special operarion time. Time:{str(end_special_column_operarion_time - start_special_column_operarion_time)}")
    return pull_action_RunID

''' ========== remove_tmp_file ========== '''
def remove_tmp_file():
    try:
        os.remove(f'{tmp_dir}/joined_{task_id}_{ts}.csv') if os.path.exists(f'{tmp_dir}/joined_{task_id}_{ts}.csv') else None
        os.remove(f'{tmp_dir}/joined_{task_id}_{ts}.sql') if os.path.exists(f'{tmp_dir}/joined_{task_id}_{ts}.sql') else None
    except Exception as e:
        logging.error("remove tmp file error:" + traceback.format_exc())
        send_task_status(task_id, TASKSTATUS_ERROR, "remove tmp file error:" + str(e))
        exit(1)

''' ========== lineage_info tracking ========== '''
def lineage_info_IDN(db_Type, db__Ip, db_Port, db_DbName, db_TableName):
    lineage_info["Dataset"]["Input"]["Datainfo"].append({
                "DbType": db_Type,
                "Ip": db__Ip,
                "Port": db_Port,
                "DbName": db_DbName,
                "TableName": db_TableName
            })
    
def lineage_info_ODN(db_Type, db__Ip, db_Port, db_DbName, db_TableName):
    lineage_info["Dataset"]["Output"]["Datainfo"].append({
                "DbType": db_Type,
                "Ip": db__Ip,
                "Port": db_Port,
                "DbName": db_DbName,
                "TableName": db_TableName
            })
    
def lineage_info_Action(action_Id, action_Type, action_Owner, action_Name, action_Desc):
    lineage_info["Action"]["ActionID"]    = action_Id
    lineage_info["Action"]["ActionType"]  = action_Type
    lineage_info["Action"]["ActionOwner"] = action_Owner
    lineage_info["Action"]["ActionName"]  = action_Name
    lineage_info["Action"]["ActionDesc"]  = action_Desc

def lineage_info_Run(run_Result, run_Msg):
    lineage_info["Run"]["Run_result"]  = run_Result
    lineage_info["Run"]["Run_endtime"] = datetime.now().timestamp()
    lineage_info["Run"]["Run_msg"]     = run_Msg
  
''' ========== clear lineage_info value ========== '''
def clear_lineage_info():
    lineage_info["Run"].clear()
    lineage_info["Action"].clear()
    lineage_info["Dataset"]["Input"]["Datainfo"].clear()
    lineage_info["Dataset"]["Input"]["Processinfo"].clear()
    lineage_info["Dataset"]["Output"]["Datainfo"].clear()
    logging.info(f"clear lineage info done.")

''' ========== pull_and_join ========== '''
def pull_and_join(task_id, task_dict):
    database_idx = 0
    one_pull = 0
    rename_col = dict()
    pull_Runid = []
    for merge_idx, merge_info in enumerate(task_dict['Merge']['Tables']):
        logging.info(f"Start processing the {merge_idx}-th merge task")
        send_task_status(task_id, TASKSTATUS_PROCESSING, f"Start processing the {merge_idx}-th merge task")
        one_pull = 0
        for i, d in enumerate(task_dict['Base']['Databases']):
            start_retrieve_time = time.time()
            if database_idx > 1 and merge_idx == 0:
                break
            if database_idx > 1 and merge_idx > 0 and i < database_idx:
                continue
            if database_idx > 1 and one_pull == 1:
                break
            if 'DbInfo' in d:
                db_info = d['DbInfo']
            if 'Condition' in d:
                db_functionCondition = d['Condition']['FunctionCondition']
                if 'SpecialFunction' in d['Condition']:
                    db_specialFunction = d['Condition']['SpecialFunction']
                else:
                    db_specialFunction = None
            else:
                db_functionCondition = None
                db_specialFunction = None

            db_connection = d['ConnectionInfo']
            db_type = db_connection['DbType']
            if database_idx > 1:
                df_index = 1
            else:
                df_index = i
            lineage_info_IDN(db_type, db_connection.get('Ip'), db_connection.get('Port'),db_info.get('DbName'), db_info.get('TableName'))
            lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
            if db_type == 'mysql':
                try:
                    logging.info("Retrieving data from MySQL")
                    lineage_info_Action("DF10-1", "LOAD", "DCFS", "Pull Mysql",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from MySQL")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    db_name  = db_info['DbName']
                    db_url   = 'mysql+pymysql://%s:%s@%s:%s/%s' % (username, password, ip, port, db_name)
                    db_engine = create_engine(db_url)
                    globals()['df%d'%df_index] = pd.read_sql(d['SqlStatement'], con=db_engine)
                    db_engine.dispose()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)   
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from MySQL: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from MySQL: " + str(e))
                    exit(1)
            elif db_type == 'mssql':
                try:
                    logging.info("Retrieving data from MSSQL")
                    lineage_info_Action("DF10-2", "LOAD", "DCFS", "Pull Mssql",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from MSSQL")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    db_name  = db_info['DbName']
                    db_url   = 'mssql+pymssql://%s:%s@%s:%s/%s' % (username, password, ip, port, db_name)
                    db_engine = create_engine(db_url)
                    globals()['df%d'%df_index] = pd.read_sql(d['SqlStatement'], con=db_engine)
                    db_engine.dispose()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1                  
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from MSSQL: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from MSSQL: " + str(e))
                    exit(1)
            elif db_type == 'phoenix':
                try:
                    logging.info("Retrieving data from Phoenix")
                    lineage_info_Action("DF10-3", "LOAD", "DCFS", "Pull Phoenix",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from Phoenix")
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    db_name  = db_info['DbName']
                    db_url   = 'phoenix://%s:%s/' % (ip, port)
                    db_engine = create_engine(db_url)
                    globals()['df%d'%df_index] = pd.read_sql(d['SqlStatement'], con=db_engine)
                    db_engine.dispose()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from Phoenix: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from Phoenix: " + str(e))
                    exit(1)
            elif db_type == 'oracle':
                try:
                    logging.info("Retrieving data from OracleDB")
                    lineage_info_Action("DF10-4", "LOAD", "DCFS", "Pull Oracle",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from OracleDB")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port'] # e.g., 1521/sid

                    db_name  = db_info['DbName'] # no need, replaced with port_sid
                    db_url   = 'oracle+cx_oracle://%s:%s@%s:%s' % (username, password, ip, port)
                    db_engine = create_engine(db_url)
                    globals()['df%d'%df_index] = pd.read_sql(d['SqlStatement'], con=db_engine)
                    db_engine.dispose()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    # Change all column names to uppercase
                    for column_name in globals()['df%d'%df_index].columns:
                        globals()['df%d'%df_index] =  globals()['df%d'%df_index].rename( columns={column_name:column_name.upper()} )

                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1                    
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from OracleDB: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from OracleDB: " + str(e))
                    exit(1)
            elif db_type == 'cassandra':
                try:
                    logging.info("Retrieving data from Cassandra")
                    lineage_info_Action("DF10-5", "LOAD", "DCFS", "Pull Cassandra",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from Cassandra")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    db_name  = db_info['DbName']
                    auth_provider = PlainTextAuthProvider(username, password)
                    cluster = Cluster([ip],port=int(port),auth_provider=auth_provider)
                    session = cluster.connect()
                    rows = session.execute(d['SqlStatement'])
                    globals()['df%d'%df_index] = pd.DataFrame(rows)
                    cluster.shutdown()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from Cassandra: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from Cassandra: " + str(e))
                    exit(1)
            elif db_type == 'elasticsearch':
                try:
                    logging.info("Retrieving data from Elasticsearch")
                    lineage_info_Action("DF10-6", "LOAD", "DCFS", "Pull Elasticsearch",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from Elasticsearch")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    index_name = db_info['TableName']
                    keynames   = db_info['ColumnsName']
                    if d['SqlStatement'] == "":
                        filter_js = {}
                    else:
                        filter_js  = json.loads(d['SqlStatement'])

                    es = Elasticsearch(hosts=ip, port=port, http_auth=(username, password))
                    es_result = helpers.scan(
                        client  = es,
                        query   = filter_js,
                        _source = keynames,
                        index   = index_name,
                        scroll  = '100m',
                        timeout = "100m"
                    )
                    rows = []
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
                        rows.append(tmp_dict)
                    globals()['df%d'%df_index] = pd.DataFrame(rows, columns=keynames)
                    rows = None
                    es.transport.close()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from Elasticsearch: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from Elasticsearch: " + str(e))
                    exit(1)
            elif db_type == 'mongodb':
                try:
                    logging.info("Retrieving data from MongoDB")
                    lineage_info_Action("DF10-7", "LOAD", "DCFS", "Pull Mongodb",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from MongoDB")
                    username = db_connection['UserName']
                    password = db_connection['Password']
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    db_name  = db_info['DbName']

                    tbl_name = db_info['TableName']
                    if d['SqlStatement'] == "":
                        filter = {}
                    else:
                        filter  = json.loads(d['SqlStatement'])

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

                    mongodb_cursor = mongodb_db[tbl_name].find(filter, projection)

                    globals()['df%d'%df_index] = pd.DataFrame(list(mongodb_cursor))
                    mongodb_cursor = None
                    mongodb_client.close()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from MongoDB: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from MongoDB: " + str(e))
                    exit(1)
            elif db_type == 'hbase':
                try:
                    logging.info("Retrieving data from HBase")
                    lineage_info_Action("DF10-8", "LOAD", "DCFS", "Pull Hbase",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "Retrieving data from HBase")
                    ip       = db_connection['Ip']
                    port     = db_connection['Port']

                    tbl_name = db_info['TableName']
                    columns  = db_info['ColumnsName']
                    query    = d['SqlStatement']

                    connection = happybase.Connection(ip, port=int(port))
                    happybase_table = happybase.Table(tbl_name, connection)
                    b_columns = [str.encode(s) for s in columns]

                    data = ()
                    if query != "":
                        data = happybase_table.scan(columns = b_columns, filter = query)
                    else:
                        data = happybase_table.scan(columns = b_columns)

                    # my_list example: ['', '65535'] ['', '111'] ['John', '9487'] ['', '111'] ['John', '9487'] ['John', '1234']
                    my_list = ((list(["" if d.get(col) is None else d[col].decode('utf-8') for col in b_columns])) for _, d in data)

                    my_data = pd.DataFrame(my_list, columns=columns)
                    globals()['df%d'%df_index] = my_data
                    my_data = None
                    connection.close()

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in retrieving data from HBase: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in retrieving data from HBase: " + str(e))
                    exit(1)
            elif db_type == 'excel':
                try:
                    logging.info("downloading xls file")
                    lineage_info_Action("DF10-9", "LOAD", "DCFS", "Pull Excel",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "downloading xls file")
                    filename = db_info['TableName']
                    columns  = db_info['ColumnsName']

                    params = {'from': f'hds:///join/upload-excel/{filename}', 'to': 'local:///'}
                    encoded = urllib.parse.urlencode(params)
                    url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}'
                    my_data = pd.read_excel(url)
                    if len(columns) > 0:
                        my_data = my_data[[x for x in columns]]
                    globals()['df%d'%df_index] = my_data
                    my_data = None

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1                   
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in downloading xls file:" + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in downloading xls file:" + str(e))
                    exit(1)
            elif db_type == 'csv':
                try:
                    logging.info("downloading csv file")
                    lineage_info_Action("DF10-10", "LOAD", "DCFS", "Pull Csv",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "downloading csv file")
                    filename = db_info['TableName']
                    columns  = db_info['ColumnsName']

                    params = {'from': f'hds:///join/upload-csv/{filename}', 'to': 'local:///'}
                    encoded = urllib.parse.urlencode(params)
                    url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}'
                    my_data = pd.read_csv(url)
                    if len(columns) > 0:
                        my_data = my_data[[x for x in columns]]
                    globals()['df%d'%df_index] = my_data
                    my_data = None

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in downloading csv file: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in downloading csv file: " + str(e))
                    exit(1)
            elif db_type == 'hds':
                try:
                    logging.info("downloading csv file")
                    lineage_info_Action("DF10-11", "LOAD", "DCFS", "Pull HDS",f"pull {db_type}")
                    send_task_status(task_id, TASKSTATUS_PROCESSING, "downloading csv file")
                    filename = db_info['TableName']
                    columns  = db_info['ColumnsName']

                    params   = {'from': f'hds:///join/csv/{filename}', 'to': 'local:///'}
                    encoded  = urllib.parse.urlencode(params)
                    url = f'http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}'
                    my_data = pd.read_csv(url)
                    if len(columns) > 0:
                        my_data = my_data[[x for x in columns]]
                    globals()['df%d'%df_index] = my_data
                    my_data = None

                    lineage_info_Run("Success", "null")
                    pull_action_RunID = str(lineage_tracing(lineage_info))
                    clear_lineage_info()
                    pull_action_RunID = special_function(df_index, db_specialFunction, pull_action_RunID)
                    pull_action_RunID = datasource_function(df_index, db_functionCondition, pull_action_RunID)
                    database_idx += 1
                    one_pull = 1       
                except Exception as e:
                    lineage_info_Run("Fail", traceback.format_exc())
                    ignore = lineage_tracing(lineage_info)
                    logging.error("Error in downloading csv file: " + traceback.format_exc())
                    send_task_status(task_id, TASKSTATUS_FAILED, "Error in downloading csv file: " + str(e))
                    exit(1)
            elif db_type == 'dataframe' or db_type == 'None':
                # keep using df0 as the left table
                pass
            else:
                logging.error("Unsupported DB type:" + db_type)
                send_task_status(task_id, TASKSTATUS_FAILED, "Unsupported DB type:" + db_type)
                exit(1)
            end_retrieve_time = time.time()
            logging.info(f'Finished retrieving table {i} from {db_type}. Time:{str(end_retrieve_time - start_retrieve_time)}')
            pull_Runid.append(pull_action_RunID)
            clear_lineage_info()
        # join
        if len(task_dict['Merge']['Tables']) < 1 or db_type == 'None':
            df_joined = globals()['df0']
        else:
            try:
                start_join_time = time.time()
                logging.info('Start joining two tables')
                send_task_status(task_id, TASKSTATUS_PROCESSING, "Start joining two tables")

                # The merge key needs to be of the same type, otherwise there will be an error
                for index_column, left_table_column in enumerate(merge_info[0]['MergeColumnName']):
                    right_table_column = merge_info[1]['MergeColumnName'][index_column]
                    if globals()['df0'].dtypes[left_table_column] != globals()['df1'].dtypes[right_table_column]:
                        globals()['df0'][left_table_column] = globals()['df0'][left_table_column].astype(object)
                        globals()['df1'][right_table_column] = globals()['df1'][right_table_column].astype(object)
                #   check if have conflicting column
                conflicting_rename(merge_info, rename_col, task_dict)

                df_joined = globals()['df0'].merge(globals()['df1'], how='left', left_on=merge_info[0]['MergeColumnName'], right_on=merge_info[1]['MergeColumnName'])

                # If the name of the column is different, delete the column in the table on the right
                for index_column, left_table_column in enumerate(merge_info[0]['MergeColumnName']):
                    right_table_column = merge_info[1]['MergeColumnName'][index_column]
                    if  left_table_column != right_table_column:
                        df_joined.drop(right_table_column, axis=1, inplace=True)

                end_join_time = time.time()
                logging.info(f'Finished joining two tables. Time:{str(end_join_time - start_join_time)}')
                send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished joining two tables")
            except Exception as e:
                logging.error("Error in merge the two tables: " + traceback.format_exc())
                send_task_status(task_id, TASKSTATUS_FAILED, "Error in merge the two tables: " + str(e))
                exit(1)
        globals()['df0'] = df_joined # reuse df0 if it's a pipeline task
    lineage_info["Dataset"]["Input"]["Processinfo"].extend(pull_Runid)
    logging.info(f'Finished joining two tables datalineage lineage_info = {lineage_info}')
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
                globals()['df1'].rename(
                    columns={conflicting_col: conflicting_col + "_" + str(rename_col.get(conflicting_col) + 1)},
                    inplace=True)
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[0]]["Column"] = conflicting_col + "_" + str(
                    rename_col.get(conflicting_col) + 1)
                rename_col[conflicting_col] += 1
                continue
            if rename_count == 1:
                globals()['df%d' % rename_item.index(1)].rename(
                    columns={conflicting_col: "*" + conflicting_col + "_" + str(index[rename_item.index(1)])},
                    inplace=True)
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[rename_item.index(1)]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[rename_item.index(1)])
            elif rename_count == 2:
                globals()['df0'].rename(columns={conflicting_col: "*" + conflicting_col + "_" + str(index[0])},
                                        inplace=True)
                globals()['df1'].rename(columns={conflicting_col: "*" + conflicting_col + "_" + str(index[1])},
                                        inplace=True)
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[0]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[0])
                task_dict['ResultParameterSetting']['ColumnTypeList'][index[1]][
                    "Column"] = "*" + conflicting_col + "_" + str(index[1])
            elif rename_count == 0:
                globals()['df0'].rename(columns={conflicting_col: conflicting_col + "_1"}, inplace=True)
                globals()['df1'].rename(columns={conflicting_col: conflicting_col + "_2"}, inplace=True)
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


''' ========== Get mission info(.json) from dcfs tmp file(from HDS) ========== '''
try:
    taskinfo_filepath = sys.argv[1]
    if taskinfo_filepath[0:6] == 'hds://':
        taskinfo_filepath = taskinfo_filepath[6:]
    with open(taskinfo_filepath, 'r') as rf:
        task_dict = json.load(rf)
    task_id = str(task_dict['TaskAction']['TaskId'])
    spark_join = str(task_dict['TaskAction']['Spark'])
    if spark_join == "True" :
        send_task_status(task_id, TASKSTATUS_ACCEPTED, "This task is handled by spark. Wait for spark to process the task.")
        exit(1)
except Exception as e:
    send_task_status(str(-1), TASKSTATUS_UNKNOWN, traceback.format_exc())
    exit(1)

''' ========== start join ========== '''
try:
    ts = str(datetime.now().timestamp())
    setup_logging('joined_' + task_id + '_' + ts + '.log')

    logging.debug('Task info filepath:' + taskinfo_filepath)
    logging.debug('Task info:' + json.dumps(task_dict))
    timeout = int(os.environ.get('DCFS_TIMEOUT', 360000000))
    clear_lineage_info()
    # Start pulling data and joining data
    df_joined = func_timeout(timeout, pull_and_join, args=(task_id, task_dict))
    lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
    lineage_info_Action("DF30-1", "MERGE", "DCFS", "Join DCFS","join DCFS")
    if df_joined is not None:
        lineage_info_Run("Success", "null")
        join_action_RunID = str(lineage_tracing(lineage_info))
except Exception as e:
    lineage_info_Run("Fail", traceback.format_exc())
    ignore = lineage_tracing(lineage_info)
    logging.error("join error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "FunctionTimedOut:" + str(e))
    exit(1)
clear_lineage_info()
''' ========== rename column ========== '''
try:
    isRename = False
    columnTypeList = task_dict['ResultParameterSetting']['ColumnTypeList']
    for column_idx, column_info in enumerate(columnTypeList):
        rename = column_info['Rename']
        if rename['IsEnable'] == "True":
            isRename=True
            lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
            df_joined = df_joined.rename(columns={column_info['Column']:rename['NewName']})
    if(isRename==True):
        lineage_info["Dataset"]["Input"]["Processinfo"].append(join_action_RunID)
        lineage_info_Run("Success", "null")
        lineage_info_Action("DF50-2", "CONVERT", "DCFS", "Rename Column","rename column")
        join_action_RunID = str(lineage_tracing(lineage_info))
except Exception as e:
    lineage_info_Run("Fail", traceback.format_exc())
    ignore = lineage_tracing(lineage_info)
    logging.error("rename column error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "rename column error:" + str(e))
    exit(1)
clear_lineage_info()

''' ========== create phoenix sql and auto add primary key(autotimestamp) if not have one ========== '''
try:
    def generate_phoenix_autotimestamp():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    # table_name = task_info['hds']['table']
    table_name = task_dict['TaskAction']['TableName'].upper()

    # create phoenix sql and check whether to add auto_timestamp column
    phoenix_sql = ''
    if task_dict['TaskAction']['Phoenix'] == "True":
        primary_key_info = None
        phoenix_sql += f'CREATE TABLE {table_name} ('
        for column_idx, column_info in enumerate(columnTypeList):
            if column_info['IsPrimaryKey'] == "True":
                primary_key_info = column_info
                if column_info['Column'] == "AUTOTIMESTAMP_":
                    autotimestamp = [generate_phoenix_autotimestamp() for _ in range(df_joined.shape[0])]
                    rename = column_info['Rename']
                    if rename['IsEnable'] == "True":
                        df_joined[rename['NewName']] = autotimestamp
                    else:
                        df_joined[column_info['Column']] = autotimestamp
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
    # logging.debug('df_joined:' + str(df_joined))

    # save to csv and phoenix sql
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_csv_path = f'{tmp_dir}/joined_{task_id}_{ts}.csv'
    tmp_sql_path = f'{tmp_dir}/joined_{task_id}_{ts}.sql'

    if phoenix_sql != "" and task_dict['TaskAction']['AppendPhoenix'] != 'True':
        with open(tmp_sql_path, 'w') as wf:
            wf.write(phoenix_sql)
except Exception as e:
    logging.error("tmp phoenix's sql and csv path error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "tmp phoenix's sql and csv path error:" + str(e))
    exit(1)

''' ========== dataframe convert to csv ========== '''
try:
    start_convert_csv_time = time.time()
    logging.debug(f'Start dataframe convert to csv.')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start dataframe convert to csv.")
    df_joined.to_csv(tmp_csv_path, index=False, header=True)

    end_convert_csv_time = time.time()
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished dataframe convert to csv.")
    logging.debug(f'Finished dataframe convert to csv. Time:{end_convert_csv_time - start_convert_csv_time}')
except Exception as e:
    logging.error(f"Error in when dataframe convert to csv:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, f"Error in when dataframe convert to csv:" + str(e))
    remove_tmp_file()
    exit(1)

''' ========== Store data(csv) into HDS ========== '''
try:
    start_hds_time = time.time()
    lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
    lineage_info["Dataset"]["Input"]["Processinfo"].append(join_action_RunID)
    lineage_info_Action("DF60-1", "STORE", "DCFS", "Store HDS","store hds")
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start importing csv file into HDS")
    logging.info("Start importing csv file into HDS")
    params = {'from': 'local:///', 'to': f'hds:///join/csv/{table_name}.csv'}
    # For special symbols like |, it needs to be encoded
    encoded = urllib.parse.urlencode(params)
    cmd = f'curl -T "{tmp_csv_path}" -L -X POST "http://{hds_host}:{hds_port}/dataservice/v1/access?{encoded}"'

    hds_error_message = "Error importing csv file into HDS. Error message:"
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    stdout, stderr = process.communicate()
    exit_code = process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    stdout_json = json.loads(stdout)
    hds_error = stdout_json['task']['state']
    lineage_info_Run("Success", "null")
    if hds_error != 'SUCCEED':
        lineage_info_Run("Fail", f"{hds_error_message} stdout:{stdout}, stderr:{stderr}")
        ignore = lineage_tracing(lineage_info)
        logging.error(f"{hds_error_message} stdout:{stdout}, stderr:{stderr}")
        send_task_status(task_id, TASKSTATUS_FAILED, f"{hds_error_message} stdout:{stdout}, stderr:{stderr}")
        exit(1)
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Finished importing csv file into HDS")
    end_hds_time = time.time()
    logging.info(f"Finished importing csv file into HDS. Time:{str(end_hds_time-start_hds_time)}")
    lineage_info_ODN("HDS", hds_host, hds_port, "hds:///join/csv/", table_name)
    ignore = lineage_tracing(lineage_info)
except Exception as e:
    logging.error(f"{hds_error_message} e:{traceback.format_exc()}, stdout:{stdout}, stderr:{stderr}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"{hds_error_message} e:{str(e)}")
    remove_tmp_file()
    exit(1)
clear_lineage_info()

''' ========== Store data(table) into Phoenix ========== '''
try:
    if task_dict['TaskAction']['Phoenix'] == 'True':
        phoenix_error_message = "Error in when importing table into Phoenix:"
        phoenix_home = "/opt/phoenix-hbase-2.4-5.1.2-bin"
        hds_ip = 'zoo1'
        start_phoenix_time = time.time()
        logging.info('Start importing table into Phoenix')
        send_task_status(task_id, TASKSTATUS_PROCESSING, "Start importing table into Phoenix")

        lineage_info["Run"]["Run_starttime"] = datetime.now().timestamp()
        lineage_info["Dataset"]["Input"]["Processinfo"].append(join_action_RunID)
        lineage_info_Action("DF60-2", "STORE", "DCFS", "Store Phoenix","store phoenix")

        cmd = phoenix_home+"/bin/psql.py -h in-line %s -t \"%s\" %s %s" % (hds_ip, table_name, tmp_sql_path, tmp_csv_path)
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
            remove_tmp_file()

            lineage_info_Run("Fail", f"{phoenix_error_message} stdout:{stdout}, stderr:{stderr}")
            ignore = lineage_tracing(lineage_info)
            exit(1)
        elif stderr.find("ERROR") != -1:
            logging.error(f"{phoenix_error_message}" + stderr)
            send_task_status(task_id, TASKSTATUS_FAILED, f"{phoenix_error_message}" + stderr)
            remove_tmp_file()

            lineage_info_Run("Fail", f"{phoenix_error_message} stdout:{stdout}, stderr:{stderr}")
            ignore = lineage_tracing(lineage_info)
            exit(1)
        lineage_info_Run("Success", "null")
        ignore = lineage_tracing(lineage_info)
except Exception as e:
    lineage_info_Run("Fail", traceback.format_exc())
    ignore = lineage_tracing(lineage_info)
    logging.error("Error in when importing table into Phoenix:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_FAILED, "Error in when importing table into Phoenix:" + str(e))
    remove_tmp_file()
    exit(1)
clear_lineage_info()


'''========== Metadata stats =========='''
check_metadata_err = False
metadata_err_arr = []
try:
    logging.info(f'Start to Metadata stats')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start to Metadata stats")
    phoenix_ip = "hbase-master1"
    conn = phoenixdb.connect(f'http://{phoenix_ip}:8765/', autocommit=True)
    cursor = conn.cursor()
    #This table_name meeting influence after Neo4j's reading
    Count_table_name = 'METADATA_STATS'

    check = upsert_counts_to_phoenix(task_dict, Count_table_name,conn)
    if check:
        check_metadata_err = True
        metadata_err_arr.append(check)
        logging.info(f"{str(check)}")

except Exception as e:
    logging.error(f"An error occurred when update column count into phoenix: {str(e)}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"Error in update Metadata stats into phoenix table: {str(e)}")
    check_metadata_err = True
    metadata_err_arr.append(e)
conn.close()

'''========== create node to neo4j =========='''

logging.info(f'Neo4j Start')
send_task_status(task_id, TASKSTATUS_PROCESSING, "Neo4j Start")
def get_column_stats_from_phoenix(phoenix_ip, Count_table_name):
    conn = phoenixdb.connect(f'http://{phoenix_ip}:8765/', autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {Count_table_name}')
    columns = [desc[0] for desc in cursor.description]
    column_stats = [dict(zip(columns, row)) for row in cursor.fetchall()]
    #column_stats = [{'Column_name': row[0], 'Db_Name': row[1], 'Table_Name': row[2], 'Column_count': row[3], 'Column_Frequency': row[4], 'Join_Count': row[5], 'Last_Used_Column': row[6]} for row in cursor.fetchall()]

    logging.debug(f"Column stats: {column_stats}")
    return column_stats


try:
    logging.info(f'Start to create node to neo4j')
    send_task_status(task_id, TASKSTATUS_PROCESSING, "Start to create node to neo4j")

    # Get columns data from input file
    with open(taskinfo_filepath, 'r') as tf:
        relation_data = json.load(tf)

    columns = []
    for db in relation_data['Base']['Databases']:
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
    logging.error(f"An error occurred when creating node to Neo4j: {str(e)}")
    send_task_status(task_id, TASKSTATUS_FAILED, f"Error in Neo4j processing: {str(e)}")
    check_metadata_err = True
    metadata_err_arr.append(e)
    
''' ========== json transfer to ELK =========='''
##ELK transfer
logging.info("Start to Create Metadata List")
with open(taskinfo_filepath, 'r') as f:
    json_data = json.load(f)

metadata_list = []
for database in json_data['Base']['Databases']:
    if database['ConnectionInfo']['DbType'] == 'None':
        continue
    metadata = get_column_names(database)
    metadata_list.append(metadata)


check = store_metadata_to_elasticsearch(metadata_list)
if check:
    check_metadata_err = True
    metadata_err_arr.append(check)


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
        logging.error(f"An error occurred when sync neo4j data to elk: {str(e)}")
        send_task_status(task_id, "TASKSTATUS_FAILED", f"Error in ELK processing: {str(e)}")
        check_metadata_err = True
        metadata_err_arr.append(e)

# sync
sync_neo4j_to_elk(task_id)

''' ========== remove tmp file ========== '''
remove_tmp_file()

''' ========== Job finished ========== '''
try:
    params = {'from': f'hds:///join/csv/{table_name}.csv', 'to': f'local:///{table_name}.csv', 'redirectfrom':'NULL'}
    encoded = urllib.parse.urlencode(params)
    if check_metadata_err:
        send_task_status(task_id, TASKSTATUS_SUCCEEDED, f"Merge job finished, but there's something wrong when storing metadata.\n {str(metadata_err_arr)}", f'/dataservice/v1/access?{encoded}', table_name)
        logging.info("Task finished, but there's something wrong when storing metadata.")
    else:
        send_task_status(task_id, TASKSTATUS_SUCCEEDED, "Job finished.", f'/dataservice/v1/access?{encoded}', table_name)
        logging.info("Job finished.")
    end = time.time()
    logging.info("Total time:" + str(end - start) + "(sec)")
except Exception as e:
    logging.error("Job finished error:" + traceback.format_exc())
    send_task_status(task_id, TASKSTATUS_ERROR, "Job finished error:" + str(e))
    exit(1)

exit()
