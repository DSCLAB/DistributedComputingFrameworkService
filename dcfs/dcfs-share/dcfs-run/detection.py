import pandas as pd
import numpy as np
import random
import math
import json
import traceback
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine
import sys
import logging
from elasticsearch import Elasticsearch
from scipy.stats import moment
import importlib.util

def setup_logging(filename):
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n',
                    filename=log_dir+filename,
                    filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s]\n%(message)s\n[%(pathname)s %(funcName)s %(lineno)d]\n')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)
    for logger_name in logging.root.manager.loggerDict:
        if logger_name != __name__:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

log_dir = "/dcfs-share/detection-logs/"
ts = str(datetime.now().timestamp())
taskinfo_filepath = sys.argv[1]

if taskinfo_filepath[0:6] == 'hds://':
    taskinfo_filepath = taskinfo_filepath[6:]
with open(taskinfo_filepath, 'r') as rf:
    line1 = rf.readline()
    line2 = rf.readline()

is_manual_execute = False
if line2:
    is_manual_execute = True
    task_id = line2.strip()
else:
    task_id = line1.strip()

setup_logging('task_' + ts + '_' + task_id + '.log')
logging.info(line1)
logging.info(line2)

try:
    logging.info('Start doing detection task.')
    logging.info('Connect to elasticsearch.')
    es_storage = {
        "es_host": "elasticsearch",
        "es_port": 9200,
        "es_user": "elastic",
        "es_password": "elastic"
    }
    detection_task_index = "data_detection_task"
    detection_result_index = "data_detection_result"
    es_client = Elasticsearch([{"host": es_storage['es_host'], "port": es_storage['es_port']}], http_auth=(es_storage['es_user'], es_storage['es_password']))

    logging.info('Connect finished.')

    detection_obj = {}

except Exception as e:
    logging.info(traceback.format_exc())
    sys.exit(0)

def chebyshev(bound):
    k = round(math.sqrt(1/bound), 2)
    return k

def get_detection_task(id):
    logging.info('Start gathering information about detection task.')

    global detection_obj
    data = es_client.get(index=detection_task_index, id=id)
    detection_obj = data['_source']
    source = detection_obj['TaskInfo']
    username, password = source["UserName"], source["Password"]
    ip, port = source["Ip"], source["Port"]
    db = source["DbName"]
    db_type = source["DbType"]
    if db_type == "mysql":
        source["db_url"] = f'mysql+pymysql://{username}:{password}@{ip}:{port}/{db}'
    elif db_type == "mssql":
        source["db_url"] = f'mssql+pymssql://{username}:{password}@{ip}:{port}/{db}'
    elif db_type == "oracle":
        source["db_url"] = f'oracle+cx_oracle://{username}:{password}@{ip}:{port}/{db}'
    logging.info(detection_obj)


def calculate_statistics_or_detection():
    logging.info('Check whether to calculate statistics or do detection.')
    global detection_obj
    logging.info('Calculate statistics.' if 'InitialStat' in detection_obj else 'Do detection.')
    return 'InitialStat' in detection_obj

# for SQL DB
def check_data_size():
    logging.info('Check detection size.')
    global detection_obj
    source = detection_obj["TaskInfo"]
    db_engine = create_engine(source["db_url"])
    db_type = source["DbType"]

    TableName = source["TableName"]
    pk = source["IntPkCol"]
    pk_tail = source["PkMax"] if pk else None
    dt = source["TimestampCol"]
    dt_tail = source["TimestampMax"] if dt else None
    if dt_tail:
        dt_tail = datetime.fromisoformat(dt_tail)
        dt_tail = dt_tail.replace(microsecond=0)

    if pk_tail:
        sql_query = f'SELECT count(*) AS row_count, max({pk}) AS max_ind FROM {TableName} WHERE {pk} > {pk_tail}'
    elif pk:
        sql_query = f'SELECT count(*) AS row_count, max({pk}) AS max_ind FROM {TableName}'
    elif dt_tail:
        if db_type == 'mysql':
            sql_query = f"SELECT count(*) AS row_count, min({dt}) AS min_ts, max({dt}) AS max_ts FROM {TableName} WHERE {dt} > '{dt_tail}'"
        elif db_type == 'mssql':
            sql_query = f"SELECT count(*) AS row_count, min({dt}) AS min_ts, max({dt}) AS max_ts FROM {TableName} WHERE {dt} > '{dt_tail}'"
        elif db_type == 'oracle':
            if source['TimestampColType'] == 'date':
                sql_query = f"SELECT count(*) AS row_count, min({dt}) AS min_ts, max({dt}) AS max_ts FROM {TableName} WHERE {dt} > TO_DATE('{dt_tail}', 'YYYY-MM-DD')"
            else:
                sql_query = f"SELECT count(*) AS row_count, min({dt}) AS min_ts, max({dt}) AS max_ts FROM {TableName} WHERE {dt} > TO_DATE('{dt_tail}', 'YYYY-MM-DD HH24:MI:SS')"
    elif dt:
        sql_query = f'SELECT count(*) AS row_count, min({dt}) AS min_ts, max({dt}) AS max_ts FROM {TableName}'
    else:
        sql_query = f'SELECT count(*) AS row_count FROM {TableName}'

    logging.info(sql_query)

    df = pd.read_sql(sql_query, con=db_engine)
    logging.info(df)
    db_engine.dispose()

    source["row_count"] = df['row_count'].iloc[0]
    logging.info('Task row count: ' + source['row_count'].astype(str))

    if pk:
        source["max_ind"] = df['max_ind'].iloc[0]
        source["min_ind"] = pk_tail+1 if pk_tail else 1
        logging.info(f'Task index range:: ({source["min_ind"]}, {source["max_ind"]})')
    if not pk and dt:
        source["max_ts"] = df['max_ts'].iloc[0]
        source["min_ts"] = df['min_ts'].iloc[0]
        logging.info(f'Task index range:: ({source["min_ts"]}, {source["max_ts"]})')

def get_sample_data(is_detection):
    logging.info('Start getting sample data')
    global detection_obj
    source = detection_obj["TaskInfo"]

    db_engine = create_engine(source["db_url"])
    row_count = source["row_count"]

    sample_num = 100000
    # sample_num = source['sample_number'] if 'sample_number' in source else 100000
    
    do_sample = True if row_count > sample_num else False
    pk = source["IntPkCol"]
    pk_tail = source["PkMax"] if pk else None
    dt = source["TimestampCol"]
    dt_tail = source["TimestampMax"] if dt else None
    if dt_tail:
        dt_tail = datetime.fromisoformat(dt_tail)
        dt_tail = dt_tail.replace(microsecond=0)
    db_type = source["DbType"]

    if not do_sample:
        sql_query = f'SELECT * FROM {source["TableName"]}'
        if is_detection:
            if pk:
                sql_query += f' WHERE {pk} > {pk_tail}'
            elif dt:
                if db_type == 'mysql' or db_type == 'mssql':
                    sql_query += f" WHERE {dt} > '{dt_tail}'"
                elif db_type == 'mssql':
                    sql_query += f" WHERE {dt} > '{dt_tail}'"
                elif db_type == 'oracle':
                    sql_query += f" WHERE {dt} > TO_DATE('{dt_tail}', 'YYYY-MM-DD HH24:MI:SS')"
    else:
        logging.info('Conduct sampling')
        if pk:
            rand = [random.randint(source['min_ind'], source['max_ind']) for _ in range(sample_num)]
            rand.sort()
            rand_cond = ','.join(map(str, rand))
            sql_query = f'SELECT * FROM {source["TableName"]} WHERE {source["IntPkCol"]} IN ({rand_cond})'
        elif dt:
            if db_type == 'mysql':
                sql_query = f"SELECT * FROM {source['TableName']} WHERE {dt} > '{dt_tail}' ORDER BY RAND() LIMIT {sample_num}"
            elif db_type == 'mssql':
                sql_query = f"SELECT TOP {sample_num} * FROM {source['TableName']} WHERE {dt} > '{dt_tail}' ORDER BY NEWID()"
            elif db_type == 'oracle':
                sql_query = f"SELECT * FROM (SELECT * FROM {source['TableName']} WHERE {dt} > TO_DATE('{dt_tail}', 'YYYY-MM-DD HH24:MI:SS') ORDER BY dbms_random.value) WHERE ROWNUM <= {sample_num}"
        else:
            if db_type == 'mysql':
                sql_query = f'SELECT * FROM {source["TableName"]} ORDER BY RAND() LIMIT {sample_num}'
            elif db_type == 'mssql':
                # percent = int(sample_num / row_count * 100)
                # if percent > 0:
                #     sql_query = f"SELECT * FROM {source['TableName']} TABLESAMPLE ({percent} PERCENT)"
                # else:
                #     sql_query = f'SELECT TOP {sample_num} * FROM {source["TableName"]} ORDER BY NEWID()'
                sql_query = f'SELECT TOP {sample_num} * FROM {source["TableName"]} ORDER BY NEWID()'
            elif db_type == 'oracle':
                # tmp = sample_num / row_count
                # percent = round(tmp, 2)
                # sql_query = f'SELECT * FROM {source["TableName"]} WHERE ROUND(DBMS_RANDOM.VALUE, 2) <= {percent}'
                sql_query = f'SELECT * FROM {source["TableName"]} WHERE ROWNUM <= {sample_num} ORDER BY dbms_random.value'
        
    df = pd.read_sql(sql_query, con=db_engine)
    logging.info(df.columns.tolist())
    logging.info(df.head(3))

    return df    

def collect_statistics(df):
    logging.info('Start collecting statistics.')
    global detection_obj
    source = detection_obj["TaskInfo"]
    result = {}

    if source['IntPkCol']:
        result['PkMin'] = source['min_ind']
        result['PkMax'] = source['max_ind']
    elif source['TimestampCol']:
        result['TimestampMax'] = source['max_ts']
        result['TimestampMin'] = source['min_ts']

    for column in df.columns.values:
        int_lower_check_list = [i.lower() for i in source["IntColCheckList"]]
        str_lower_check_list = [i.lower() for i in source["StrColCheckList"]]
        userdef_lower_check_list = []
        lower_col = column.lower()
        if "UserDefColCheckList" in source:
            userdef_lower_check_list = [i.lower() for i in source["StrColCheckList"]]

        if lower_col in int_lower_check_list:
            tmp_data = df[column]
            mean_value = tmp_data.mean()
            var_value = tmp_data.var()
            min_value = tmp_data.min()
            max_value = tmp_data.max()

            percent = np.percentile(tmp_data, [i for i in range(0, 101, 10)])
            percent_arr = [round(i, 2) for i in percent]
            moment_dic = {}
            if "moment" in source:
                for i in range(3, source["moment"]+1):
                    moment_dic[i] = moment(tmp_data, i)

            origin_col = source["IntColCheckList"][int_lower_check_list.index(lower_col)]
            result[origin_col] = {
                'DataType': 'num',
                'Mean': round(mean_value),
                'Var': round(math.sqrt(var_value)),
                'Min': min_value,
                'Max': max_value,
                'Percentiles': percent_arr
            }
            if 'ChebyshevBound' in source and origin_col in source['ChebyshevBound']:
                result[origin_col]['ChebyshevBound'] = chebyshev(source['ChebyshevBound'][origin_col])
            if len(moment_dic):
                result[origin_col]['moment'] = moment_dic
        elif lower_col in str_lower_check_list:
            if pd.api.types.is_numeric_dtype(df[column]):
                df[column] = df[column].astype(str)
            var_counts = df[column].value_counts().keys().tolist()

            origin_col = source["StrColCheckList"][str_lower_check_list.index(lower_col)]
            result[origin_col] = {
                'DataType': 'category',
                'CategorySet': var_counts
            }
        elif lower_col in userdef_lower_check_list:
            global_ns = {
                'data': df[column]
            }
            exec(source["UserDefColSetting"][column]["StatisticFuncContent"], global_ns)

            code = f'res = {source["UserDefColSetting"][column]["StatisticFuncName"]}(data)'
            exec(code, global_ns)

            origin_col = source["UserDefColCheckList"][userdef_lower_check_list.index(lower_col)]
            result[origin_col] = {
                'DataType': 'custom',
                'Data': global_ns['res']
            }
    if source["IntPkCol"]:
        source["PkMax"] = df.iloc[-1].get(source["IntPkCol"])
    elif source["TimestampCol"]:
        source["TimestampMax"] = df.iloc[-1].get(source["TimestampCol"])

    logging.info(result)
    detection_obj['InitialStat'] = result

def record_statistics(task_id):
    logging.info('Start record statistics.')
    logging.info(detection_obj)
    update_body = {
        "doc": {
            'InitialStat': detection_obj['InitialStat'],
            "TaskInfo": {}
        }
    }

    if detection_obj["TaskInfo"]["IntPkCol"]:
        update_body["doc"]["TaskInfo"]["PkMax"] = detection_obj["TaskInfo"]["max_ind"]
    elif detection_obj["TaskInfo"]["TimestampCol"]:
        update_body["doc"]["TaskInfo"]["TimestampMax"] = detection_obj["TaskInfo"]["max_ts"]

    logging.info(update_body)
    res = es_client.update(index=detection_task_index, id=task_id, body=update_body)
    logging.info(res)

def detect_data_quality(df):
    logging.info('Start executing detection task.')

    global detection_obj
    source = detection_obj['TaskInfo']
    result = {
        'TaskID': source['TaskID'],
        'TaskName': source['TaskName'],
        'TaskStatus': True,
        'Timestamp': None,
        'ValidationResult': True,
        #'SampleNum': df.shape[0],
        'Result': {
            'SampleNum': df.shape[0],
            'Detail': {}
        },
        'ExecutionType': 'Manual' if is_manual_execute else 'Auto'       
    }
    stat = detection_obj['InitialStat']

    if source['IntPkCol']:
        result['Result']['PkMin'] = source['min_ind']
        result['Result']['PkMax'] = source['max_ind']
    elif source['TimestampCol']:
        result['Result']['TimestampMax'] = source['max_ts']
        result['Result']['TimestampMin'] = source['min_ts']
    #result['Result']['SampleNum'] = df.shape[0]

    for col in df.columns.values:
        int_lower_check_list = [i.lower() for i in source["IntColCheckList"]]
        str_lower_check_list = [i.lower() for i in source["StrColCheckList"]]
        userdef_lower_check_list = []
        if "UserDefColCheckList" in source:
            userdef_lower_check_list = [i.lower() for i in source["StrColCheckList"]]

        lower_col = col.lower()
        if lower_col in int_lower_check_list:
            tmp_data = df[col]
            mean_value = tmp_data.mean()
            var_value = tmp_data.var()

            if np.isnan(var_value):
                logging.info('nan')
                var_value = 0

            min_value = tmp_data.min()
            max_value = tmp_data.max()

            origin_col = source["IntColCheckList"][int_lower_check_list.index(lower_col)]

            percent = np.percentile(df[col], [i for i in range(0, 101, 10)])
            percent_arr = [round(i, 2) for i in percent]

            col_stat = stat[origin_col]
            statistic_mean = col_stat["Mean"]
            statistic_var = col_stat["Var"]

            moment_dic = {}
            if "moment" in source:
                for i in range(3, source["moment"]+1):
                    moment_dic[i] = moment(df[col], i)

            bound_3var_upper = statistic_mean + statistic_var * 3
            bound_3var_lower = statistic_mean - statistic_var * 3
            bound_5var_upper = statistic_mean + statistic_var * 5
            bound_5var_lower = statistic_mean - statistic_var * 5

            detect_list = tmp_data.tolist()
            ValueOutOf3var = [i for i in detect_list if i > bound_3var_upper or i < bound_3var_lower]
            ValueOutOf5var = [i for i in detect_list if i > bound_5var_upper or i < bound_5var_lower]

            if 'ChebyshevBound' in col_stat:
                bound_number = col_stat['ChebyshevBound']
                bound_cust_upper = statistic_mean + statistic_var * bound_number
                bound_cust_lower = statistic_mean - statistic_var * bound_number
                ValueOutOfCustom = [i for i in detect_list if i > bound_cust_upper or i < bound_cust_lower]

            tmp_dic = {
                'DataType': 'num',
                'Mean': round(mean_value),
                'Var': round(math.sqrt(var_value)),
                'Min': min_value,
                'Max': max_value,
                'Percentiles': percent_arr,
                'ValueOutOf3var': ValueOutOf3var,
                'ValueOutOf5var': ValueOutOf5var,
                'IsPass': True
            }
            if len(moment_dic):
                tmp_dic['moment'] = moment_dic

            '''if 'ChebyshevBound' in col_stat:
                tmp_dic['ValueOutOfCustomBound'] = ValueOutOfCustom
                if len(ValueOutOfCustom) > (result['SampleNum'] * source['ChebyshevBound'][col]):
                    tmp_dic['IsPass'] = False
                    result['ValidationResult'] = False
            elif len(ValueOutOf3var) > (result['SampleNum'] / 9):
                tmp_dic['IsPass'] = False
                result['ValidationResult'] = False'''
            
            if 'ChebyshevBound' in col_stat:
                tmp_dic['ValueOutOfCustomBound'] = ValueOutOfCustom
                if len(ValueOutOfCustom) > (result['Result']['SampleNum'] * source['ChebyshevBound'][col]):
                    tmp_dic['IsPass'] = False
                    result['ValidationResult'] = False
            elif len(ValueOutOf3var) > (result['Result']['SampleNum'] / 9):
                tmp_dic['IsPass'] = False
                result['ValidationResult'] = False
            result['Result']['Detail'][origin_col] = tmp_dic

        elif lower_col in str_lower_check_list:
            origin_col = source["StrColCheckList"][str_lower_check_list.index(lower_col)]
            col_stat = stat[origin_col]

            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].astype(str)
            detect_list = df[col].tolist()
            item_out_of_set = [i for i in detect_list if i not in col_stat['CategorySet']]

            tmp_dic = {
                'DataType': 'category',
                'ValueOutOfSet': item_out_of_set,
                'IsPass': True
            }
            if len(item_out_of_set):
                tmp_dic['IsPass'] = False
                result['ValidationResult'] = False
            result['Result']['Detail'][origin_col] = tmp_dic

        elif lower_col in userdef_lower_check_list:
            origin_col = source["UserDefColCheckList"][userdef_lower_check_list.index(lower_col)]

            global_ns = {
                'data': df[col],
                'stat': stat[col]['Data']
            }
            exec(source["UserDefColSetting"][col]["DetectionFuncContent"], global_ns)

            code = f'res = {source["UserDefColSetting"][col]["DetectionFuncName"]}(data, stat)'
            exec(code, global_ns)
            tmp_dic = {
                'DataType': 'custom',
                'Log': global_ns['res'][1],
                'IsPass': global_ns['res'][0]
            }

            if not tmp_dic['IsPass']:
                result['ValidationResult'] = False
            result['Result']['Detail'][origin_col] = tmp_dic

    result['Timestamp'] = datetime.now().isoformat()
    logging.info(result)
    return result

def record_detection(record, task_id):
    logging.info('Start recording detection result.')
    global detection_obj
    source = detection_obj['TaskInfo']
    record['Timestamp'] = datetime.now().isoformat()
    logging.info(record)

    update_task_body = {
        "doc": {
            "TaskInfo": {}
        }  
    }

    if not is_manual_execute:
        if source['IntPkCol']:
            update_task_body['doc']['TaskInfo']['PkMax'] = source['max_ind']
        elif source['TimestampCol']:
            update_task_body['doc']['TaskInfo']['TimestampMax'] = source['max_ts']

        if source['ExpectCount']:
            update_task_body['doc']['TaskInfo']['ExecutionCount'] = source['ExecutionCount'] + 1
            record['Iteration'] = source['ExecutionCount'] + 1
            if record['ValidationResult']:
                update_task_body['doc']['TaskInfo']['SuccessCount'] = source['SuccessCount'] + 1
            else:
                update_task_body['doc']['TaskInfo']['FailCount'] = source['FailCount'] + 1

            if source['ExpectCount'] == source['ExecutionCount']:
                update_task_body['doc']['TaskInfo']['Enable'] = False
    else:
        record['Iteration'] = -1

    logging.info(update_task_body)
    res = es_client.index(index=detection_result_index, body=record)
    res2 = es_client.update(index=detection_task_index, id=task_id, body=update_task_body)
    logging.info(res)
    logging.info(res2)

def record_none_detection(task_id):
    logging.info('No new data created between two detection periods.')
    global detection_obj
    global is_manual_execute
    source = detection_obj["TaskInfo"]

    record = {
        "TaskID": source["TaskID"],
        "TaskName": source["TaskName"],
        "TaskStatus": True,
        "ValidationResult": False,
        "Iteration": -1,
        "Result": {},
        "ErrorMsg": "No new data between two detection periods.",
        "ExecutionType": "Manual" if is_manual_execute else "Auto",
        "Timestamp": datetime.now().isoformat()
    }
    if not is_manual_execute:
        if source['ExpectCount']:
            record['Iteration'] = source['ExecutionCount'] + 1

    res = es_client.index(index=detection_result_index, body=record)
    logging.info(res)

    update_body = {
        "doc": {
            "TaskInfo": {
            }
        }  
    }

    if not is_manual_execute:
        if source['ExpectCount']:
            update_body['doc']['TaskInfo']['ExecutionCount'] = source['ExecutionCount'] + 1
            update_body['doc']['TaskInfo']['FailCount'] = source['FailCount'] + 1
            if source['ExpectCount'] == source['ExecutionCount']:
                update_body['doc']['TaskInfo']['Enable'] = False
    res = es_client.update(index=detection_task_index, id=task_id, body=update_body)
    logging.info(res)

def record_error_status(msg):
    try:
        if not is_manual_execute:
            update_task_body = {
                "doc": {
                    "TaskInfo": {
                        "ExecutionCount": detection_obj['TaskInfo']['ExecutionCount'] + 1,
                        "FailCount": detection_obj['TaskInfo']['FailCount'] + 1
                    }
                }  
            }
            logging.info(update_task_body)
            res = es_client.update(index=detection_task_index, id=task_id, body=update_task_body)
            logging.info(res)

        record = {
            'TaskID': detection_obj['TaskInfo']['TaskID'],
            'TaskName': detection_obj['TaskInfo']['TaskName'],
            'Timestamp': datetime.now().isoformat(),
            'TaskStatus': False,
            'ValidationResult': False,
            'Iteration': (detection_obj['TaskInfo']['ExecutionCount'] + 1) if not is_manual_execute else -1,
            'Result': {},
            'ExecutionType': 'Manual' if is_manual_execute else 'Auto',
            "ErrorMsg": str(msg)
        }
        logging.info(record)
        res2 = es_client.index(index=detection_result_index, body=record)
        logging.info(res2)

        es_client.close()
    except Exception as e:
        logging.info(e)
        logging.info(traceback.format_exc())

try:
    get_detection_task(task_id)
    is_detection = calculate_statistics_or_detection()
    check_data_size()
    df = get_sample_data(is_detection)
    if is_detection:
        if detection_obj["TaskInfo"]["row_count"] == 0:
            record_none_detection(task_id)
        else:
            report = detect_data_quality(df)
            record_detection(report, task_id)
    else:
        collect_statistics(df)
        record_statistics(task_id)

    logging.info("Detection task end.")
    es_client.close()
except Exception as e:
    logging.info(e)
    logging.info(traceback.format_exc())
    record_error_status(e)
    sys.exit(0)
