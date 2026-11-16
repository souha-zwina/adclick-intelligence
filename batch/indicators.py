#!/usr/bin/env python3
"""Voie froide : lit l'historique Parquet dans HDFS avec Spark et calcule les premiers indicateurs.

Indicateurs produits :
  summary         : volume, événements distincts (détection de doublons), clics, taux de clic (CTR)
  by_partition    : nombre de messages par partition Kafka (contrôle de l'ingestion)
  missing         : taux de valeurs manquantes par colonne I* et C*
  numeric_stats   : moyenne, min, max de chaque variable numérique I*
  ctr_by_c1       : taux de clic par valeur de C1 (les plus fréquentes)

Les résultats sont affichés et sauvegardés en Parquet dans <HDFS_URL>/indicators/<nom>.
Lancement (voir README) : docker compose run --rm spark-job /app/batch/indicators.py
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INT_COLS = ["I%d" % i for i in range(1, 14)]
CAT_COLS = ["C%d" % i for i in range(1, 27)]


def ctr_by(df, column, top=20, min_count=50):
    """Taux de clic par valeur d'une colonne (valeurs ayant au moins min_count impressions)."""
    return (
        df.groupBy(column)
        .agg(F.count("*").alias("impressions"), F.sum("label").alias("clicks"))
        .where(F.col("impressions") >= min_count)
        .withColumn("ctr", F.col("clicks") / F.col("impressions"))
        .orderBy(F.col("impressions").desc())
        .limit(top)
    )


def compute_indicators(df):
    """Retourne un dictionnaire {nom: DataFrame} des indicateurs batch."""
    summary = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("distinct_events"),
        F.sum("label").alias("clicks"),
        F.min("event_id").alias("min_event_id"),
        F.max("event_id").alias("max_event_id"),
    ).withColumn("ctr", F.col("clicks") / F.col("rows"))

    by_partition = df.groupBy("kafka_partition").count().orderBy("kafka_partition")

    all_cols = INT_COLS + CAT_COLS
    missing_wide = df.agg(*[F.avg(F.col(c).isNull().cast("double")).alias(c) for c in all_cols])
    stack = "stack(%d, %s) as (column, missing_rate)" % (
        len(all_cols), ", ".join("'%s', `%s`" % (c, c) for c in all_cols))
    missing = missing_wide.select(F.expr(stack))

    stats_wide = df.agg(*[
        f(F.col(c).cast("double")).alias("%s_%s" % (c, name))
        for c in INT_COLS for name, f in (("mean", F.avg), ("min", F.min), ("max", F.max))
    ])
    stack = "stack(%d, %s) as (column, mean, min, max)" % (
        len(INT_COLS), ", ".join("'%s', `%s_mean`, `%s_min`, `%s_max`" % (c, c, c, c) for c in INT_COLS))
    numeric_stats = stats_wide.select(F.expr(stack))

    return {
        "summary": summary,
        "by_partition": by_partition,
        "missing": missing,
        "numeric_stats": numeric_stats,
        "ctr_by_c1": ctr_by(df, "C1"),
    }


def main():
    hdfs = os.getenv("HDFS_URL", "hdfs://namenode:9000")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=hdfs + "/data/impressions")
    ap.add_argument("--output", default=hdfs + "/indicators")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("adclick-indicators").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    print("Historique lu : %s" % args.input)
    df.printSchema()

    for name, result in compute_indicators(df).items():
        print("\n=== %s ===" % name)
        result.show(50, truncate=False)
        result.write.mode("overwrite").parquet("%s/%s" % (args.output, name))
    print("\nIndicateurs sauvegardés dans %s" % args.output)


if __name__ == "__main__":
    main()
