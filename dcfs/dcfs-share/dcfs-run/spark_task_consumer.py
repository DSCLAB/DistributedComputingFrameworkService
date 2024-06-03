#!/usr/bin/env python
import pika, sys, os
import urllib.parse
import json
import time
import traceback
import subprocess

dcfs_fail = "dcfs_fail"

''' ========== send json error ========== '''
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

def main():
    try_times = 100000
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

    channel.queue_declare(queue=dcfs_fail)

    def callback(ch, method, properties, body):
        try:
            # body: {abc.json}.The memory usage limit has been exceeded too many times and the Computing Box will be killed.
            body = urllib.parse.unquote(body.decode('utf-8'))
            task_id = body.split(".")[0]
            send_task_status(task_id, TASKSTATUS_ACCEPTED, "This task is handled by spark. Wait for spark to process the task.")
        except:
            send_task_status(str(-1), TASKSTATUS_UNKNOWN, traceback.format_exc())

        try:
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
            send_task_status(task_id, TASKSTATUS_ERROR, "Error in submit spark:" + traceback.format_exc())

    channel.basic_consume(queue=dcfs_fail, on_message_callback=callback, auto_ack=True)
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