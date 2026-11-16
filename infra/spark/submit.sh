#!/bin/sh
# Soumet un job PySpark au cluster Spark (master + 2 workers), en mode client.
# Usage : /opt/submit.sh /app/<dossier>/<job>.py [arguments du job]
set -eu

# Adresse que les workers utiliseront pour joindre le driver (ce conteneur)
DRIVER_HOST="$(python3 -c 'import socket; print(socket.gethostbyname(socket.gethostname()))')"

# spark.hadoop.dfs.replication=2 : sans cela, le client Spark écrit avec le défaut de Hadoop (3)
# et, avec seulement 2 DataNodes, les blocs resteraient "under-replicated".
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --conf spark.driver.host="${DRIVER_HOST}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.sql.shuffle.partitions=3 \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  --conf spark.executor.memory=512m \
  --conf spark.ui.showConsoleProgress=false \
  "$@"
