#!/usr/bin/env python3
"""Voie froide : archive le topic Kafka dans HDFS au format Parquet (Spark Structured Streaming).

Kafka (topic ad-impressions) --> parse du JSON + typage --> Parquet dans HDFS.

- Source   : Kafka (obligatoire d'après la fiche). Les messages sont des JSON dont toutes les
             valeurs I*, C* et label sont des chaînes ; le typage est fait ici.
- Sortie   : <HDFS_URL>/data/impressions (Parquet, mode append).
- Reprise  : le checkpoint (<HDFS_URL>/checkpoints/archiver) mémorise les offsets Kafka déjà
             traités. Si le job est arrêté puis relancé, il reprend où il s'était arrêté.
- Garantie : écriture "exactement une fois" vers le sink Parquet (source Kafka rejouable +
             checkpoint). Les doublons viennent seulement d'un renvoi des mêmes lignes par le
             producteur ; ils se détectent avec event_id (voir batch/indicators.py).

Lancement (voir README) : docker compose up -d archiver
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

INT_COLS = ["I%d" % i for i in range(1, 14)]    # variables numériques
CAT_COLS = ["C%d" % i for i in range(1, 27)]    # variables catégorielles hachées

# Schéma des messages JSON envoyés par ingestion/producer.py
EVENT_SCHEMA = StructType(
    [StructField("label", StringType())]
    + [StructField(c, StringType()) for c in INT_COLS + CAT_COLS]
    + [StructField("event_id", LongType()), StructField("event_time", DoubleType())]
)


def parse_events(raw):
    """Transforme les messages Kafka bruts (colonnes value, partition, offset, timestamp)
    en table typée. Les messages illisibles (sans event_id) sont ignorés."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).where(F.col("e.event_id").isNotNull())

    columns = [
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_time").cast("timestamp").alias("event_time"),   # epoch (s) -> timestamp
        F.col("e.label").cast("int").alias("label"),
    ]
    columns += [F.col("e." + c).cast("int").alias(c) for c in INT_COLS]
    columns += [F.col("e." + c).alias(c) for c in CAT_COLS]
    columns += [F.col("kafka_partition"), F.col("kafka_offset"), F.col("kafka_timestamp")]
    return parsed.select(*columns)


def main():
    hdfs = os.getenv("HDFS_URL", "hdfs://namenode:9000")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "ad-impressions"))
    ap.add_argument("--output", default=hdfs + "/data/impressions")
    ap.add_argument("--checkpoint", default=hdfs + "/checkpoints/archiver")
    ap.add_argument("--trigger-seconds", type=int, default=30, help="intervalle entre deux micro-lots")
    ap.add_argument("--max-offsets", type=int, default=100000, help="messages max par micro-lot")
    ap.add_argument("--available-now", action="store_true",
                    help="traite tout ce qui est disponible puis s'arrête (utile pour constituer l'historique)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("adclick-kafka-to-hdfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")      # utilisé seulement au tout premier démarrage
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )

    writer = (
        parse_events(raw).writeStream.format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="%d seconds" % args.trigger_seconds)

    query = writer.start()
    print("Archivage démarré : %s -> %s (checkpoint %s)" % (args.topic, args.output, args.checkpoint), flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
