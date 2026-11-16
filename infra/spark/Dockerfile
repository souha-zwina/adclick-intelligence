# Image Spark du cluster (master, workers et jobs) : Spark 3.5.6 + connecteur Kafka.
# Le connecteur est téléchargé À LA CONSTRUCTION de l'image (pas à chaque job).
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

ARG SPARK_VERSION=3.5.6
ARG MVN=https://repo1.maven.org/maven2

# Source Kafka pour Spark Structured Streaming et ses dépendances
ADD ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Les fichiers téléchargés par ADD ne sont lisibles que par root : on les rend lisibles
RUN chmod 644 /opt/spark/jars/spark-sql-kafka-0-10_2.12-*.jar \
              /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-*.jar \
              /opt/spark/jars/kafka-clients-3.4.1.jar \
              /opt/spark/jars/commons-pool2-2.11.1.jar

# Script de soumission commun à tous les jobs (supprime les fins de ligne Windows éventuelles)
COPY submit.sh /opt/submit.sh
RUN sed -i 's/\r$//' /opt/submit.sh && chmod 755 /opt/submit.sh

USER spark
