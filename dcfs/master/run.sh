#!/bin/bash

trap '$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR --daemon stop resourcemanager' EXIT INT TERM

# Safe mode is OFF in namenode1/172.22.0.30:8020
# Safe mode is OFF in namenode2/172.22.0.21:8020
is_safemode=$(hdfs dfsadmin -safemode get)
retry=1000
while [[ $is_safemode == *"ON"* && $retry != 0 ]]
do
    echo "HDFS is in Safe mode, wait 3s. retry = ${retry}"
    retry=$(($retry-1))
    sleep 3
    is_safemode=$(hdfs dfsadmin -safemode get)
done

# Remove the application from RMStateStore.
echo "Formats the RMStateStore. This will clear the RMStateStore and is useful if past applications are no longer needed."
yarn resourcemanager -format-state-store

$HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR --daemon start resourcemanager

nohup python3 /dcfs-share/dcfs-run/task_consumer.py &
nohup python3 /dcfs-share/dcfs-run/spark_task_consumer.py &
nohup python3 /dcfs-share/dcfs-run/detection_consumer.py &
nohup python3 /dcfs-share/dcfs-run/detection_trigger.py &

hostname=$(hostname)

check=3000
while [[ $check != 0 && $(yarn application -list | grep "DCFS") != *"DCFS"* ]]
do
    # Failed to connect: Call From dcfs-master1/172.22.0.42 to dcfs-master1:8033 failed on connection exception: java.net.ConnectException: Connection refused;
    active_rm=$(yarn rmadmin -getAllServiceState | grep $hostname)
    # hostname:dcfs-master2,active_rm:dcfs-master2:8033                                  active    
    echo "hostname:$hostname,active_rm:$active_rm"
    if [[ $active_rm == *"$hostname"* && $active_rm == *"active"* ]]; then
        echo "Select active RM to start dcfs, because dcfs only starts one"
        sh /dcfs-job/mainDCFS.sh
        sh /dcfs-job/detection.sh
        echo "Create spark log path"
        /opt/hadoop-3.3.4/bin/hadoop fs -mkdir /spark
        /opt/hadoop-3.3.4/bin/hadoop fs -mkdir /spark/eventLog
        # /opt/hadoop-3.3.4/bin/hadoop fs -mkdir /spark/logDirectory
        echo "Select active RM to start spark history server, because spark history server only starts one"
        bash /spark/sbin/start-history-server.sh
        break
    else
        echo "$hostname is standby"
    fi
    check=$(($check-1))
    sleep 1
done

tail -F $HADOOP_HOME/logs/hadoop-$USER-resourcemanager-$(hostname).log &
child=$!
wait "$child"