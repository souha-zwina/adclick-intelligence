#!/usr/bin/env python3
"""Consommateur de contrôle : lit le topic et affiche les premiers messages et un comptage.

Lancer plusieurs instances avec le même --group pour voir les partitions réparties entre
consommateurs ; avec des --group différents, chaque groupe reçoit TOUS les messages.
"""
import argparse
import json
import os
from collections import Counter

from kafka import KafkaConsumer


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'))
    ap.add_argument('--topic', default=os.getenv('KAFKA_TOPIC', 'ad-impressions'))
    ap.add_argument('--group', default='check-group')
    ap.add_argument('--show', type=int, default=3, help='nombre de messages à afficher')
    ap.add_argument('--timeout', type=int, default=10, help='arrêt après N s sans nouveau message')
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=args.timeout * 1000,
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
    )
    total, per_partition, clicks = 0, Counter(), 0
    for msg in consumer:
        total += 1
        per_partition[msg.partition] += 1
        clicks += 1 if msg.value.get('label') == '1' else 0
        if total <= args.show:
            print('partition=%d offset=%d %s' % (msg.partition, msg.offset, json.dumps(msg.value)[:200] + '...'))
    print('Total lu : %d | par partition : %s | clics (label=1) : %d' % (total, dict(per_partition), clicks))


if __name__ == '__main__':
    main()
