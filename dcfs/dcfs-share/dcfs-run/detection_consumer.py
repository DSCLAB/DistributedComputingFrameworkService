import pika, sys, os
import urllib.parse
import json
import time
import traceback
import subprocess
import random
from datetime import datetime
import logging
import socket

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
    setup_logging('detection.log')

    try_times = 1000
    while try_times > 0:
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
#            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
            channel = connection.channel()
            break
        except:
            try_times -= 1
            if try_times >= 1:
                print("Connection failed. Retry in 3 seconds")
                time.sleep(3)

    channel.queue_declare(queue='detection')
    channel.queue_declare(queue='detection_manual')

    def callback(ch, method, properties, body):
        print("Receive message in detection queue.")
        try:
            task_id = urllib.parse.unquote(body.decode('utf-8'))
            logging.info(task_id)
            txt_file = f'/dcfs-share/detection-task/{task_id}.txt'
            with open(txt_file, 'w') as wf:
                wf.write(task_id)

            curr = datetime.now().timestamp()

            params = {'from': 'local:///', 'to': f'hds:///detection/task/{task_id}.txt'}
            encoded = urllib.parse.urlencode(params)
            cmd = f'curl --data-binary "@{txt_file}" -L POST "http://hbase-regionserver1:8000/dataservice/v1/access?{encoded}"'
            logging.info(cmd)
            process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
        except:
            print("Detection task consuming fails.")
            logging.info(traceback.format_exc())

    def callback2(ch, method, properties, body):
        print("Receive message in detection_manual queue.")
        try:
            task_id = urllib.parse.unquote(body.decode('utf-8'))
            logging.info(task_id)
            txt_file = f'/dcfs-share/detection-task/{task_id}.txt'
            with open(txt_file, 'w') as wf:
                wf.write("manual\n")
                wf.write(task_id)

            curr = datetime.now().timestamp()

            params = {'from': 'local:///', 'to': f'hds:///detection/task/{task_id}.txt'}
            encoded = urllib.parse.urlencode(params)
            cmd = f'curl --data-binary "@{txt_file}" -L POST "http://hbase-regionserver1:8000/dataservice/v1/access?{encoded}"'
            logging.info(cmd)
            process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process.wait()
            
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
        except:
            print("Detection task consuming fails.")
            logging.info(traceback.format_exc())


    channel.basic_consume(queue='detection', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='detection_manual', on_message_callback=callback2, auto_ack=True)

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
