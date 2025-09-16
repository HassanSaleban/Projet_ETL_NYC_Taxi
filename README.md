# Projet_ETL_NYC_Tax# 🚖 Projet ETL NYC Taxi Data avec Airflow + Polars

## 📌 Objectif
Mettre en place un pipeline **ETL orchestré par Airflow 3** pour :
1. **Télécharger** les fichiers Parquet des taxis (yellow / green / fhv) depuis le cloud NYC TLC.
2. **Transformer** les fichiers (normalisation du schéma, typage, ajout des colonnes manquantes).
3. **Concaténer** les fichiers mensuels en fichiers annuels (2012 → 2025).
4. Stocker les résultats dans des dossiers montés via Docker (`exports/`).

---

## 📂 Architecture du projet

```
Airflow/
│── dags/                        # Tous les DAGs Airflow
│   ├── taxi_download_dag.py      # Téléchargement taxis yellow/green
│   ├── taxi_fhv_download_dag.py  # Téléchargement taxis FHV (via CSV)
│   ├── transform_yellow_parquet.py  # Transformation des yellow
│   ├── yellow_concat_dag.py      # Concaténation annuelle des yellow
│
│── scripts/
│   ├── nyc_tlc_parquet_links.csv # Liens TLC officiels (utilisé pour FHV)
│   └── utils_*.py                # Fonctions utilitaires (download, transform…)
│
│── exports/                      # Volume monté (entrée/sortie)
│   ├── staging/                  # Fichiers bruts téléchargés
│   │   ├── yellow/<année>/*.parquet
│   │   ├── green/<année>/*.parquet
│   │   └── fhv/<année>/*.parquet
│   ├── transformation/           # Fichiers transformés (schéma homogène)
│   │   └── yellow/<année>/*.parquet
│   └── concatenation/            # Fichiers annuels concaténés
│       └── yellow_year_<année>.parquet
│
│── dockerfile                    # Image Airflow custom (avec Polars, pyodbc…)
│── docker-compose.yaml           # Orchestration des services Airflow
│── .env                          # Variables d’environnement (chemins, etc.)
```

---

## 🛠️ Étapes du pipeline

### 1. Téléchargement (`taxi_download_dag.py`, `taxi_fhv_download_dag.py`)
- **Yellow / Green** : génération d’URL par année+mois.
- **FHV** : lecture du CSV `nyc_tlc_parquet_links.csv` pour récupérer les liens exacts.
- Gestion robuste avec :
  - `requests.Session` + `Retry`
  - Vérification `HEAD` avant download
  - Skip si fichier déjà présent et complet
  - Extraction des métadonnées Parquet en JSON

---

### 2. Transformation (`utils_transformations_yellow.py` + `transform_yellow_parquet.py`)
- Normalisation des schémas avec **Polars** :
  - Cast explicite (`Int32`, `Float64`, `Datetime(us)`, etc.)
  - Colonnes manquantes ajoutées avec `NULL`
  - Colonnes réordonnées
- Idempotence :
  - Vérifie si le fichier existe déjà en sortie
  - Ne réécrit pas inutilement

---

### 3. Concaténation (`yellow_concat_dag.py`)
- Parcourt tous les Parquets transformés d’une année
- Concatène avec `pl.concat()` (gestion automatique des colonnes manquantes → `null`)
- Produit un fichier unique par année :
  ```
  exports/concatenation/yellow_year_<année>.parquet
  ```

---

## 🚀 Déploiement avec Docker

### 1. Construire l’image
```bash
docker build -t airflow-taxi .
```

### 2. Lancer Airflow avec Docker Compose
```bash
docker-compose up -d
```

### 3. Monter les volumes
Dans `docker-compose.yaml` :
```yaml
volumes:
  - ./exports:/opt/airflow/exports
  - ./dags:/opt/airflow/dags
  - ./scripts:/opt/airflow/scripts
```

Ainsi, les chemins deviennent POSIX dans les DAGs :
```
/opt/airflow/exports/staging
/opt/airflow/exports/transformation
/opt/airflow/exports/concatenation
```

---

## 📊 DAGs disponibles

### 🔹 Téléchargement
- **`taxi_green_download_dag`** → télécharge green (2009–2025)
- **`taxi_fhv_download_dag`** → télécharge fhv selon CSV
- **`taxi_download_dag`** → télécharge yellow

### 🔹 Transformation
- **`transform_yellow_parquet`** → homogénéise les Parquets yellow

### 🔹 Concaténation
- **`yellow_concat_daily`** → concatène les fichiers transformés en fichiers annuels

---

## ✅ Bonnes pratiques appliquées
- **Idempotence** : pas de réécriture inutile si fichiers déjà présents
- **Robustesse** : retry réseau, validation tailles téléchargées
- **Modularité** : séparation utils / DAGs
- **POSIX paths** : pour compatibilité entre Docker/Linux et Windows
- **Dynamic Task Mapping** : parallélisation par année ou mois
