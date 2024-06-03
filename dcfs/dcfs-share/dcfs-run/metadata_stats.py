import json
import logging
import phoenixdb
import traceback
from datetime import datetime, timedelta
from collections import Counter
from collections import defaultdict

# logging.basicConfig(level=logging.DEBUG)


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
        return e
    