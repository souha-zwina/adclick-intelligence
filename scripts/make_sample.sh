#!/bin/sh
# Usage : scripts/make_sample.sh /chemin/vers/train.txt [nb_lignes]
# Crée data/sample.tsv avec les N premières lignes du fichier Criteo (défaut : 2 000 000).
set -eu
SRC="${1:?Usage: $0 chemin/train.txt [nb_lignes]}"
N="${2:-2000000}"
mkdir -p data
head -n "$N" "$SRC" > data/sample.tsv
echo "data/sample.tsv créé : $(wc -l < data/sample.tsv) lignes"
