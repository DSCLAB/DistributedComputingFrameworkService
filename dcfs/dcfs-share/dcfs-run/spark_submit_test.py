import subprocess

try:
    cmd = f"""/spark/bin/spark-submit --master yarn --deploy-mode client \
              --conf spark.dynamicAllocation.enabled=true \
              --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
              /dcfs-share/dcfs-run/spark_join.py true 7bc7f8f3-f7d8-4b6a-8ae7-d2c76d38fc29.json"""
    process = subprocess.Popen(cmd, shell=True)
except Exception as e:
    print(str(e))