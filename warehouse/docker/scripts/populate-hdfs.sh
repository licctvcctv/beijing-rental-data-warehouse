#!/bin/bash

set -euo pipefail

echo "--------------- INITIALIZE HDFS ---------------"

until "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
  sleep 3
done

"$HADOOP_HOME/bin/hdfs" dfs -chmod -R 1777 /
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/root
"$HADOOP_HOME/bin/hdfs" dfs -chown root:hadoop /user/root
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /user
