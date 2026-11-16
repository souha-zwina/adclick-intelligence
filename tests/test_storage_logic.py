"""Test local (Spark en mode local, sans Docker ni Kafka) de la logique de la couche stockage.

Vérifie, avec les messages EXACTEMENT au format du producteur (ingestion/producer.py) :
  1. parse_events : typage, valeurs manquantes, rejet des messages illisibles ;
  2. l'écriture puis la relecture Parquet ;
  3. les indicateurs batch, comparés à un calcul en Python pur.

Lancement :  pip install pyspark==3.5.6 pytest && python -m pytest tests -q
"""
import json
import os
import random
import sys
import tempfile
from datetime import datetime

import pytest

# Les workers Spark locaux doivent utiliser le même Python que le test (sinon PYTHON_VERSION_MISMATCH)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for sub in ("ingestion", "streaming", "batch"):
    sys.path.insert(0, os.path.join(ROOT, sub))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import indicators  # noqa: E402
import kafka_to_hdfs  # noqa: E402
import producer  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("test-stockage")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def fake_tsv_lines(n, seed=1):
    """Lignes au format Criteo, avec cases vides (valeurs manquantes)."""
    rnd = random.Random(seed)
    pool = ["%08x" % rnd.getrandbits(32) for _ in range(5)]
    lines = []
    for _ in range(n):
        label = "1" if rnd.random() < 0.25 else "0"
        nums = ["" if rnd.random() < 0.15 else str(rnd.randint(-2, 500)) for _ in range(13)]
        cats = ["" if rnd.random() < 0.05 else rnd.choice(pool) for _ in range(26)]
        lines.append("\t".join([label] + nums + cats) + "\n")
    return lines


def raw_kafka_df(spark, n=300):
    """Imite ce que lit Spark : value = JSON produit par producer.parse_line, plus partition/offset."""
    rows = []
    for i, line in enumerate(fake_tsv_lines(n)):
        event = producer.parse_line(line, i)
        rows.append((json.dumps(event).encode("utf-8"), i % 3, i // 3, datetime(2026, 11, 13, 12, 0, 0)))
    rows.append((b"{ceci n'est pas du json", 0, 999, datetime(2026, 11, 13, 12, 0, 0)))   # message illisible
    return spark.createDataFrame(rows, ["value", "partition", "offset", "timestamp"]), n


def test_parse_events_types_and_rejection(spark):
    raw, n = raw_kafka_df(spark)
    df = kafka_to_hdfs.parse_events(raw)
    types = dict(df.dtypes)
    assert df.count() == n                                    # le message illisible est écarté
    assert types["event_id"] == "bigint" and types["label"] == "int"
    assert types["I1"] == "int" and types["C1"] == "string"
    assert types["event_time"] == "timestamp"
    assert len(df.columns) == 3 + 13 + 26 + 3                 # 3 clés + I + C + 3 métadonnées Kafka
    assert df.where("I1 is null").count() > 0                 # les cases vides restent des null


def test_parquet_roundtrip_and_indicators(spark):
    raw, n = raw_kafka_df(spark)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "impressions")
        kafka_to_hdfs.parse_events(raw).write.mode("append").parquet(path)
        df = spark.read.parquet(path)
        res = {k: v for k, v in indicators.compute_indicators(df).items()}

        # Vérité terrain calculée en Python pur sur les mêmes lignes
        lines = fake_tsv_lines(n)
        clicks = sum(1 for l in lines if l.split("\t")[0] == "1")
        miss_i1 = sum(1 for l in lines if l.split("\t")[1] == "") / n

        s = res["summary"].collect()[0]
        assert s["rows"] == n and s["distinct_events"] == n
        assert s["clicks"] == clicks
        assert s["ctr"] == pytest.approx(clicks / n)
        assert s["min_event_id"] == 0 and s["max_event_id"] == n - 1

        parts = {r["kafka_partition"]: r["count"] for r in res["by_partition"].collect()}
        assert sum(parts.values()) == n and set(parts) == {0, 1, 2}

        missing = {r["column"]: r["missing_rate"] for r in res["missing"].collect()}
        assert len(missing) == 39 and missing["I1"] == pytest.approx(miss_i1)

        assert res["numeric_stats"].count() == 13
        assert res["ctr_by_c1"].count() > 0
