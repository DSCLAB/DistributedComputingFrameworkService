#!/bin/bash

target_file="detection_trigger.py"

pid=$(ps aux | grep "$target_file" | grep -v grep | awk '{print $2}')

if [ -z "$pid" ]; then
    echo "No finding target process"
else
  kill "$pid"
  echo "Stop detection trigger process"
fi

nohup python3 /dcfs-share/dcfs-run/detection_trigger.py &
