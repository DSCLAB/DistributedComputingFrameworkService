#!/usr/bin/env python
import pika, sys, os
import urllib.parse
import json
import time
import traceback
import subprocess
import random

''' ========== send error to Flask ========== '''
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

''' ========== Task request consumner ========== '''
def main():
    TASKSTATUS_UNKNOWN = 6
    TASKSTATUS_ERROR = 5
    try_times = 1000
    while try_times > 0:
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
            channel = connection.channel()
            break
        except:
            try_times -= 1
            if try_times >= 1:
                print("Connection failed. Retry in 3 seconds")
                time.sleep(3)

    channel.queue_declare(queue='task_req')

    def callback(ch, method, properties, body):
        try:
            error_message = "Error importing json file into HDS. Error message:"
            body = urllib.parse.unquote(body.decode('utf-8'))
            task_info = json.loads(body)
            task_id = task_info['TaskAction']['TaskId']
            spark_join = task_info['TaskAction']['Spark']

            json_filepath = f'/dcfs-share/dcfs-tmp-json/{task_id}.json'
            with open(json_filepath, 'w') as wf:
                wf.write(body)
            
            # Send json to HDS, because the file needs to be DCFS watch in a distributed environment
            params = {'from': 'local:///', 'to': f'hds:///join/json/{task_id}.json'}
            encoded = urllib.parse.urlencode(params)
            cmd = f'curl --data-binary "@{json_filepath}" -L POST "http://hbase-regionserver1:8000/dataservice/v1/access?{encoded}"'
            process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
            stdout_json = json.loads(stdout)
            hds_error = stdout_json['task']['state']
            if hds_error != 'SUCCEED':
                send_task_status(task_id, TASKSTATUS_ERROR, "task_consumer.py error:" + f"{error_message}, stdout:{stdout}, stderr:{stderr}")
            
            if spark_join == "True":
                cmd = f"""/spark/bin/spark-submit --master yarn --deploy-mode cluster \
                --conf spark.yarn.submit.waitAppCompletion=false \
                --conf spark.network.timeout=1200000s \
                --conf spark.executor.heartbeatInterval=1000000s \
                --conf spark.shuffle.io.retryWait=60s \
                --conf spark.shuffle.io.maxRetries=10 \
                --conf spark.driver.memory=4g  \
                --conf spark.driver.memoryOverhead=0 \
                --conf spark.executor.instances=3 \
                --conf spark.executor.memory=3g \
                --conf spark.executor.memoryOverhead=0 \
                --conf spark.sql.shuffle.partitions=200 \
                --conf spark.yarn.maxAppAttempts=1 \
                --conf spark.driver.maxResultSize=0 \
                --conf spark.yarn.submit.file.replication=1 \
                --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.sql.adaptive.skewJoin.enabled=true \
                --conf spark.eventLog.enabled=true \
                --conf spark.eventLog.dir=hdfs:///spark/eventLog \
                --conf spark.ui.enabled=false \
                --conf spark.sql.pivotMaxValues=10000 \
                /dcfs-share/dcfs-run/spark_join.py true {task_id}.json"""
                subprocess.Popen(cmd, shell=True)
        except:
            send_task_status(str(random.random()), TASKSTATUS_UNKNOWN, "task_consumer.py error:" + traceback.format_exc())

    channel.basic_consume(queue='task_req', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
