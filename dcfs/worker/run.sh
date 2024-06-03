#!/bin/bash

trap '$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR --daemon stop nodemanager' EXIT INT TERM

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR --daemon start nodemanager

# Add a few more consumers, so that it will be faster and HA to get RMQ information
nohup python3 /dcfs-share/dcfs-run/task_consumer.py &

nohup python3 /dcfs-share/dcfs-run/spark_task_consumer.py &

tail -F $HADOOP_HOME/logs/hadoop-$USER-nodemanager-$(hostname).log &
child=$!
wait "$child"