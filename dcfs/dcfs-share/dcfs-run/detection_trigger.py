import pika
import sys
# import sys, os
# import urllib.parse
# import json
import time
import schedule

import traceback
# import subprocess
# import random
from datetime import datetime, timedelta
import logging
import socket
from elasticsearch import Elasticsearch

def setup_logging(filename):
    log_dir = "/dcfs-share/detection-logs/"
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

def main():
    def check_task_status():
        curr = datetime.now()
        detection_index = "data_detection"
        es_storage = {
            "host": "elasticsearch",
            "port": 9200,
            "user": "elastic",
            "password": "elastic"
        }

        es_client = Elasticsearch([{"host": es_storage['host'], "port": es_storage['port']}], http_auth=(es_storage['user'], es_storage['password']))
        query = {
            "query": {
                "bool": {
                "must": [
                    {
                        "range": {
                            "TaskInfo.NextTime": {
                                "lt": curr.isoformat()
                            }
                        }
                    },
                    {
                        "term": {
                            "TaskInfo.Enable": True
                        }
                    }
                ]
                }
            }
        }
        detection_task_index = "data_detection_task"
        logging.info(query)

        result = es_client.search(index=detection_task_index, body=query)
        arr = [i['_id'] for i in result['hits']['hits']]
        logging.info(result)
        logging.info(f"Pending tasks: [{', '.join(arr)}]")

        detect_credentials = pika.PlainCredentials('guest', 'guest')
        detect_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=detect_credentials))
        detect_channel = detect_connection.channel()
        detect_channel.queue_declare(queue='detection')
        
        
        for doc in result['hits']['hits']:
            nxt = datetime.fromisoformat(doc['_source']['TaskInfo']['NextTime'])
            nxt += timedelta(hours=doc['_source']['TaskInfo']['Intervals'])
            update_body = {
                "doc": {
                    "TaskInfo": {
                        "NextTime": nxt.isoformat()
                    }
                }
            }
            detect_channel.basic_publish(exchange='', routing_key='detection', body=str(doc['_id']))
            res = es_client.update(index=detection_task_index, id=doc['_id'], body=update_body)
            logging.info(res)

        es_client.close()

    def create_detection_index(sleep_interval=0):
        if sleep_interval:
            time.sleep(sleep_interval)

        detection_task_index = "data_detection_task"
        detection_result_index = "data_detection_result"

        es_storage = {
            "host": "elasticsearch",
            "port": 9200,
            "user": "elastic",
            "password": "elastic"
        }
        es_client = Elasticsearch([{"host": es_storage['host'], "port": es_storage['port']}], http_auth=(es_storage['user'], es_storage['password']))

        while True:
            check_index_count = 2
            try:
                if es_client.indices.exists(index=detection_task_index):
                    check_index_count -= 1
                else:
                    res = es_client.indices.create(index=detection_task_index)
                    if res["acknowleged"]:
                        check_index_count -= 1
                    else:
                        time.sleep(5)

                if es_client.indices.exists(index=detection_result_index):
                    check_index_count -= 1
                else:
                    res = es_client.indices.create(index=detection_result_index)
                    if res["acknowleged"]:
                        check_index_count -= 1
                    else:
                        time.sleep(5)

                if not check_index_count:
                    break
            except Exception as e:
                print(traceback.format_exc())
                time.sleep(60)

        es_client.close()

    hostname = socket.gethostname()
    if hostname == "dcfs-master1":
        schedule.every().hour.at(":05").do(check_task_status)
        schedule.every().hour.at(":15").do(check_task_status)
        schedule.every().hour.at(":25").do(check_task_status)
        schedule.every().hour.at(":35").do(check_task_status)
        schedule.every().hour.at(":45").do(check_task_status)
        schedule.every().hour.at(":55").do(check_task_status)
        create_detection_index()
    elif hostname == "dcfs-master2":
        schedule.every().hour.at(":00").do(check_task_status)
        schedule.every().hour.at(":10").do(check_task_status)
        schedule.every().hour.at(":20").do(check_task_status)
        schedule.every().hour.at(":30").do(check_task_status)
        schedule.every().hour.at(":40").do(check_task_status)
        schedule.every().hour.at(":50").do(check_task_status)
        create_detection_index(600)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == '__main__':
    setup_logging('detection.log')
    try:
        main()
    except Exception as e:
        print(e)
        logging.info(traceback.format_exc())