/opt/hadoop-3.3.4/bin/hadoop jar /dcfs-job/dcfsCore.jar com.nlab.dcfs.tools.JobTool --name DetectionJob --configpath /dcfs-job/dcfs-detection-config.xml --files /dcfs-job/dcfsCore.jar --mode watch --autoscaling on --autoresizing on --datasource hds:///detection/task --commands python3 /dcfs-share/dcfs-run/detection.py dcfsInput
