#!/usr/bin/env python3
"""Génère un faux fichier au format Criteo (POUR TESTER SANS TÉLÉCHARGER LE DATASET).
Usage : python scripts/generate_fake_sample.py [nb_lignes] [sortie]
Ne jamais utiliser ces données pour les résultats du projet.
"""
import random
import sys

n = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
out = sys.argv[2] if len(sys.argv) > 2 else 'data/sample.tsv'
random.seed(42)
pool = ['%08x' % random.getrandbits(32) for _ in range(500)]
with open(out, 'w') as f:
    for _ in range(n):
        label = '1' if random.random() < 0.25 else '0'
        nums = [('' if random.random() < 0.15 else str(int(random.expovariate(0.1)))) for _ in range(13)]
        cats = [('' if random.random() < 0.05 else random.choice(pool)) for _ in range(26)]
        f.write('\t'.join([label] + nums + cats) + '\n')
print('%d lignes écrites dans %s' % (n, out))
