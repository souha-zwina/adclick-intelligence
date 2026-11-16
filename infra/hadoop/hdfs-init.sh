#!/bin/bash
# Initialisation de HDFS : attend les 2 DataNodes puis crée l'arborescence du projet.
# Lancé par le service « hdfs-init » du docker-compose.yml, puis le conteneur s'arrête.
# (Script dans un fichier : l'entrypoint des images bde2020 découpe mal les commandes "bash -c".)

echo "Attente des 2 DataNodes..."
until hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (2)"; do
  echo "attente des 2 DataNodes..."
  sleep 5
done

set -e
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /data/impressions /checkpoints /models /indicators
hdfs dfs -chmod -R 777 /data /checkpoints /models /indicators
hdfs dfs -ls /
echo "HDFS prêt : 2 DataNodes, réplication 2"
