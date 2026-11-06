#!/usr/bin/env python3
"""Producteur Kafka : rejoue un fichier Criteo (TSV) sous forme d'événements JSON.

Chaque ligne du fichier devient un événement (une impression publicitaire) :
  label, I1..I13 (numériques), C1..C26 (catégorielles hachées)
auxquels on ajoute deux champs SIMULÉS (le fichier d'origine n'en a pas) :
  event_id   : numéro de ligne dans le fichier (identifiant unique)
  event_time : heure d'envoi (epoch, secondes) au moment du rejeu
"""
import argparse
import json
import os
import sys
import time

COLUMNS = ['label'] + ['I%d' % i for i in range(1, 14)] + ['C%d' % i for i in range(1, 27)]


def parse_line(line, n):
    """Transforme une ligne TSV en dictionnaire ; None si la ligne est mal formée."""
    vals = line.rstrip('\n').split('\t')
    if len(vals) != len(COLUMNS):
        return None
    row = {c: (v if v != '' else None) for c, v in zip(COLUMNS, vals)}
    row['event_id'] = n
    row['event_time'] = time.time()
    return row


def make_producer(bootstrap, retries=30):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                key_serializer=lambda k: str(k).encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                linger_ms=20,
            )
        except NoBrokersAvailable:
            print('Kafka indisponible (%d/%d), nouvel essai dans 2 s...' % (attempt, retries), file=sys.stderr)
            time.sleep(2)
    sys.exit('Impossible de se connecter à Kafka : %s' % bootstrap)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--file', default=os.getenv('SAMPLE_FILE', '/data/sample.tsv'), help='fichier TSV source')
    ap.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'))
    ap.add_argument('--topic', default=os.getenv('KAFKA_TOPIC', 'ad-impressions'))
    ap.add_argument('--rate', type=float, default=0, help='messages/seconde (0 = vitesse maximale)')
    ap.add_argument('--start', type=int, default=0, help='première ligne à envoyer (incluse)')
    ap.add_argument('--end', type=int, default=None, help='dernière ligne (exclue)')
    ap.add_argument('--dry-run', action='store_true', help='affiche le JSON sans contacter Kafka')
    args = ap.parse_args()

    producer = None if args.dry_run else make_producer(args.bootstrap)
    sent = skipped = 0
    t0 = time.time()
    try:
        with open(args.file, encoding='utf-8') as f:
            for n, line in enumerate(f):
                if n < args.start:
                    continue
                if args.end is not None and n >= args.end:
                    break
                row = parse_line(line, n)
                if row is None:
                    skipped += 1
                    continue
                if producer is None:
                    print(json.dumps(row))
                else:
                    producer.send(args.topic, key=row['event_id'], value=row)
                sent += 1
                if sent % 10000 == 0:
                    print('%d événements envoyés' % sent, file=sys.stderr)
                if args.rate > 0:  # pacing : on attend l'heure théorique du prochain envoi
                    delay = t0 + sent / args.rate - time.time()
                    if delay > 0:
                        time.sleep(delay)
    except KeyboardInterrupt:
        print("Interrompu par l'utilisateur.", file=sys.stderr)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
    dur = max(time.time() - t0, 1e-9)
    print('Terminé : %d envoyés, %d lignes ignorées, %.1f s, %.0f msg/s' % (sent, skipped, dur, sent / dur),
          file=sys.stderr)


if __name__ == '__main__':
    main()
