# AdClick Intelligence – Couche d'ingestion (livrable 2)

Projet « Pipeline Big Data de bout en bout » (modules Big Data Plateformes et Big Data Analytics) réalisé par **Souhaila ELMOUJIBI** et **Hajar AITLAKSSIR**.

Objectif du projet : prédire le clic sur une publicité (classification binaire) à partir du dataset **Criteo Display Advertising Challenge**.

> **Périmètre de ce livrable : uniquement la couche d'ingestion.** Ce dépôt contient un broker **Kafka** et un **producteur Python** qui rejoue un échantillon du dataset sous forme d'événements. Les couches stockage (HDFS), traitement (Spark), ML et restitution seront ajoutées dans les livrables suivants.
>
> **La démonstration de ce livrable a été faite sur des données FICTIVES** générées au format Criteo (`scripts/generate_fake_sample.py`). Le vrai dataset Criteo sera utilisé pour les couches suivantes, après validation du sujet et des données par l'enseignant. Les statistiques affichées dans ce livrable (par exemple le nombre de clics) ne sont donc pas des résultats.

## 1. Architecture de la couche d'ingestion

```
data/sample.tsv ──► producer.py ──► Kafka (topic ad-impressions, 3 partitions) ──► consommateurs
 (échantillon          (JSON, 1 événement          KRaft, sans Zookeeper           (check_topic.py ici ;
  Criteo)               par impression)                                             Spark Structured Streaming ensuite)
```

| Service | Image | Rôle |
|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | Broker en mode KRaft (broker + contrôleur) |
| `kafka-init` | `apache/kafka:3.7.0` | Crée le topic `ad-impressions` (3 partitions) puis s'arrête |
| `producer` | build `./ingestion` (Python 3.11) | Rejoue le fichier TSV vers Kafka (profil `tools`) |
| `consumer-check` | build `./ingestion` | Lit le topic pour vérifier le contenu (profil `tools`) |

## 2. Données

- Source : Criteo Display Advertising Challenge (Kaggle / Criteo AI Lab).
- **Conditions d'utilisation** : dataset destiné à la recherche et à un usage **non commercial** (licence indiquée par Criteo : CC BY-NC-SA 3.0 ; à reconfirmer sur la page officielle du dataset et à citer dans le rapport). Notre usage est strictement pédagogique.
- Fichier d'origine `train.txt` : TSV, colonne 1 = `label` (1 = clic), colonnes 2–14 = `I1…I13` (numériques, valeurs manquantes), colonnes 15–40 = `C1…C26` (catégorielles hachées et anonymisées).
- Volume : environ 45 millions de lignes (≈ 11 Go). Nous travaillerons sur un **échantillon** (les 2 000 000 premières lignes) pour tenir sur des machines d'étudiants.
- Sens d'un événement : une impression publicitaire affichée à un internaute ; `label` indique s'il a cliqué.
- Le fichier d'origine n'a **ni identifiant ni horodatage** : le producteur ajoute `event_id` (numéro de ligne) et `event_time` (heure d'envoi). Ces deux champs sont **simulés**.
- Variables explicatives : `I1…I13` et `C1…C26`, toutes disponibles au moment de la prédiction. Cible : `label`.
- Pour la démonstration actuelle : faux échantillon (`generate_fake_sample.py`), **à ne jamais utiliser pour les résultats du projet**.

### Format d'un événement (JSON)

```json
{"label": "0", "I1": "1", "I2": null, "...": "...", "C1": "68fd1e64", "C2": "80e26c9b", "...": "...",
 "event_id": 12, "event_time": 1762358400.123}
```

Les valeurs manquantes sont envoyées à `null`. Les champs `I*` et `label` sont des chaînes à ce stade : le typage est fait côté Spark.

## 3. Prérequis

- Docker Desktop (ou Docker Engine) avec Docker Compose v2, ~4 Go de RAM disponibles pour cette couche. Sous Windows, Docker Desktop avec WSL2.
- Python 3.11 (ou `py` sous Windows) pour générer le faux échantillon.
- Pour les vraies données : le fichier `train.txt` de Criteo (non fourni dans ce dépôt).

## 4. Lancement pas à pas

### Linux / macOS / Git Bash / WSL

```bash
# 1. Variables d'environnement
cp .env.example .env

# 2. Échantillon
python scripts/generate_fake_sample.py 50000 data/sample.tsv     # DONNÉES FICTIVES (test)
#    ou, avec le vrai dataset :
scripts/make_sample.sh /chemin/vers/train.txt 2000000
```

### Windows (PowerShell)

```powershell
copy .env.example .env
python scripts\generate_fake_sample.py 50000 data\sample.tsv     # DONNÉES FICTIVES (test)
# vrai dataset :
python -c "import itertools; open('data/sample.tsv','w',encoding='utf-8',newline='\n').writelines(itertools.islice(open(r'C:\chemin\train.txt',encoding='utf-8'),2000000))"
```

Vérifier que `data/sample.tsv` n'est pas vide (`dir data` : taille supérieure à 0).

### Démarrage de Kafka et test (toutes plateformes)

```bash
# 3. Démarrer Kafka et créer le topic
docker compose up -d kafka kafka-init
docker compose logs kafka-init          # DOIT afficher « PartitionCount: 3 »

# 4. Publier des événements (ex. 20 000 lignes à 500 msg/s)
docker compose run --rm producer producer.py --rate 500 --end 20000

# 5. Vérifier ce qui est dans Kafka
docker compose run --rm consumer-check check_topic.py --show 3 --group test1 --timeout 20
```

Résultat attendu à l'étape 5 : `Total lu : 20000 | par partition : {0: ~6700, 1: ~6700, 2: ~6700} | clics (label=1) : ...`.

### Pièges connus

- **Vérifiez `PartitionCount: 3` avant de lancer le producteur ou un consommateur.** Si un client se connecte avant que `kafka-init` ait créé le topic, Kafka le crée automatiquement avec **1 seule partition** (tous les messages arrivent alors dans la partition 0). Correction :
  ```bash
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic ad-impressions
  docker compose up kafka-init
  ```
- Un consommateur reprend là où son groupe s'est arrêté : pour relire le topic depuis le début, utilisez un **nouveau nom de groupe** (`--group test2`, etc.). Sinon il affiche `Total lu : 0`.
- Si `data/sample.tsv` est vide (0 octet), le producteur n'envoie rien : régénérez le fichier.

Options du producteur : `--rate` (messages/s, 0 = vitesse max), `--start` / `--end` (plage de lignes), `--topic`, `--dry-run` (affiche le JSON sans Kafka).

Découpage prévu pour la suite : lignes 0 → 1 600 000 pour constituer l'historique (envoi rapide), lignes 1 600 000 → 2 000 000 pour la démonstration en direct (20 à 50 msg/s).

### Observer plusieurs consommateurs

```bash
# Même groupe dans deux terminaux : les 3 partitions se répartissent entre les deux consommateurs
docker compose run --rm consumer-check check_topic.py --group g1
# Groupe différent : il reçoit tous les messages indépendamment
docker compose run --rm consumer-check check_topic.py --group g2
```

Depuis la machine hôte, Kafka est joignable sur `localhost:29092`.

### Arrêt et nettoyage

```bash
docker compose down        # arrête les conteneurs (les messages Kafka sont perdus : pas de volume pour cette couche)
```

## 5. Concepts Kafka utilisés

- **Topic** : flux nommé d'événements (`ad-impressions`), divisé en **partitions** (3) pour le parallélisme.
- **Clé** : le producteur utilise `event_id` comme clé, ce qui répartit les messages entre les partitions.
- **Producteurs / consommateurs multiples** : plusieurs producteurs peuvent écrire dans le même topic ; plusieurs consommateurs peuvent lire, soit en se partageant les partitions (même groupe), soit chacun l'intégralité (groupes différents). C'est ce qui permettra à Spark (archivage et inférence) de lire le même flux indépendamment.
- **Garanties** : `acks=all` côté producteur ; un seul broker ici (facteur de réplication 1), ce qui est suffisant pour un cluster simulé.

## 6. Structure du dépôt

```
infra/ (à venir : HDFS, Spark)   ingestion/ (producer.py, check_topic.py, Dockerfile)
scripts/ (échantillon)           data/ (non versionné)      docs/ (rapport, contributions)
docker-compose.yml               .env.example               README.md
```

## 7. Limites connues et suite

- `event_id` et `event_time` sont simulés ; `event_time` représente l'heure du rejeu, pas celle de l'impression réelle. Pour le découpage temporel train/test, l'ordre de `event_id` fera foi.
- Pas de persistance des données Kafka entre deux `docker compose down` (suffisant ici, l'archivage durable se fera dans HDFS).
- Ce livrable a été validé sur données fictives ; le sujet et le dataset restent à faire valider par l'enseignant.
- Prochaines étapes : cluster HDFS (NameNode + 2 DataNodes) et Spark (master + 2 workers), job Spark Structured Streaming Kafka → HDFS (Parquet), analyses batch, entraînement Spark ML, inférence en streaming, PostgreSQL, tableau de bord.

## 8. Contributions

- Souhaila ELMOUJIBI
- Hajar AITLAKSSIR
