# 🚲 **JeVelibererLaData – Vélib Analytics Platform**  
### *Pipeline complet : 250 Go JSON.gz → ETL Python → PostgreSQL → API FastAPI → Vue analytique des trajets*

---

## 📌 **Description générale**

**JeVelibererLaData** est un projet complet de **Data Engineering + API Design**, dont l’objectif est d’extraire, transformer et analyser les données Vélib issues d’une source JSON privée (environ **250 Go** de fichiers `.json.gz`).

Le projet inclut :

- Un pipeline **ETL** robuste (Python + Jupyter)
- Un stockage modélisé en **PostgreSQL**
- Une API **FastAPI** exposant :
  - les **dimensions** (CRUD complet : stations, vélos)
  - les **faits** (lecture analytique)
  - une **vue V_TRAJETS** permettant d’analyser les déplacements
- Plusieurs **visualisations** : ERD, pipeline, swagger, architecture
- Un dépôt Git structuré et prêt pour production

Ce projet démontre des compétences en :
✔ Ingénierie Data  
✔ Modélisation SQL  
✔ Traitement de très gros volumes  
✔ Développement d’API  
✔ Documentation technique  

---

# 🏗️ **Architecture globale du pipeline**
```
                +-------------------------+
                |  Fichiers Vélib JSON.gz |
                |     (250 Go bruts)      |
                +------------+------------+
                             |
                             | 1. Extraction / Lecture
                             v
                   +--------------------+
                   |  ETL Python        |
                   | (Jupyter Notebook) |
                   +---------+----------+
                             |
                             | 2. Transformation
                             v
                     +------------------+
                     | PostgreSQL (DB)  |
                     | Dimensions/Faits |
                     +---------+--------+
                               |
                               | 3. Exposition API REST
                               v
                       +----------------+
                       |    FastAPI     |
                       |  (Uvicorn)     |
                       +-------+--------+
                               |
                               | 4. Usage / Analyse
                               v
                    +------------------------------+
                    | Swagger UI / Clients externes |
                    +------------------------------+
```
---

# 📚 **Technologies utilisées**

### 🐍 **Python**
- Jupyter Notebook (ETL)
- gzip / json
- pandas (optionnel)
- FastAPI
- Uvicorn  
- asyncpg ou psycopg2

### 🗃️ **Base de données**
- PostgreSQL  
- Modèle dimensionnel + faits  
- Vue analytique `V_TRAJETS`

### 🌐 **API Backend**
- FastAPI  
- OpenAPI 3.1  
- Validation Pydantic  

---

# 📁 **Structure du repository**

```
JeVelibererLaData/
│
├── app/
│ ├── main.py # Point d’entrée FastAPI
│ ├── models.py # Modèles SQLAlchemy / Pydantic
│ ├── etl.py # Pipeline ETL
│ ├── database.py # Connexion PostgreSQL
│ ├── routers/ # (optionnel)
│ └── init.py
│
├── data/ # Non versionné (poids énorme)
│ ├── progress.json
│ ├── data_import_errors.log
│
├── image/ # Captures d'écran
│ ├── swagger.png
│ ├── pipeline.png
│ ├── erd.png
│ └── api_structure.png
│
├── requirements.txt
├── .gitignore
├── README.md
└── LICENSE
```


---

# 🗄️ **Modèle de données PostgreSQL**

### 📌 Tables de **dimension**
- `station`  
- `velo`  
- `etat_station`  
- `localisation_velo`  
- `snapshot`  

### 📌 Vue analytique
- `V_TRAJETS`  
Reconstitution des trajets en comparant les changements de localisation des vélos.

---

# 📘 **ERD (Diagramme relationnel)**
---

# 🔄 Pipeline ETL – Architecture détaillée

Le pipeline d’ingestion traite **250 Go de fichiers JSON.gz** provenant des snapshots Vélib.  
Objectifs : *lecture → parsing → transformation → insertion incrémentale → résilience → génération des vues analytiques*.

---

## 1️⃣ **Lecture & Extraction**

- Parcours récursif du dossier source contenant les fichiers `.json.gz`
- Détection automatique du format :
  - lecture directe si `.json`
  - décompression via `gzip` si `.gz`
- Gestion d’un fichier `progress.json` pour :
  - savoir quels fichiers ont déjà été traités
  - permettre la reprise après crash
  - éviter les doublons et retraitements

---

## 2️⃣ **Parsing & Validation**

Chaque fichier contient un snapshot complet Vélib.  
Le pipeline extrait et valide trois grandes familles d’entités :

### **🅐 Station**
- station_code  
- géolocalisation (lat/lon)  
- capacité totale  
- type de station  

### **🅑 Vélo**
- velo_name  
- type (mécanique / électrique)  
- statut  

### **🅒 État & Localisation**
- état temps réel d’une station  
- localisation d’un vélo  
- timestamp exact du snapshot  

Toute erreur de format est automatiquement envoyée dans :
```
data/data_import_errors.log
```

> *(ajoutez votre image dans `/image/erd.png`)*


---

## 3️⃣ **Transformation & Normalisation**

- Nettoyage des champs  
- Renommage cohérent  
- Transformation des types Python → PostgreSQL  
- Enrichissement :
  - génération de `snapshot_id`
  - normalisation code station / bikeStatus / state

---

## 4️⃣ **Buffers mémoire (Batch Processing)**

Pour optimiser les performances, le pipeline utilise des **buffers temporaires** :

- `station_buffer`
- `velo_buffer`
- `etat_station_buffer`
- `localisation_velo_buffer`

Chaque buffer est vidé en base via **Bulk Insert**.

---

## 5️⃣ **Chargement en base (PostgreSQL)**

Insertion optimisée :

- **UPSERT** pour les dimensions station / vélo
- **INSERT batch** pour :
  - `etat_station`
  - `localisation_velo`
  - `snapshot`

Gestion transactionnelle complète :
- `commit` sur succès  
- `rollback` si erreur  

---

## 6️⃣ **Résilience & Reprise**

Composant clé : `progress.json`

Il permet :

| Fonction | Description |
|---------|-------------|
| Suivi | Liste des fichiers déjà traités |
| Reprise | Redémarrer après crash |
| Cohérence | Empêcher reprocessing d'un fichier |
| Reconstruction | Récupérer le dernier snapshot_id |

---

## 7️⃣ **Sortie : Base prête pour Analytics**

Une fois tous les fichiers traités, la base contient :

### **Tables brutes**
- `station`
- `velo`
- `snapshot`
- `etat_station`
- `localisation_velo`

### **Vue analytique reconstruite**
- `V_TRAJETS`  
Reconstitue les trajets en comparant la localisation d’un vélo entre deux snapshots consécutifs.

Exemples de métriques disponibles :

- Liste des trajets d’un vélo
- Temps moyen d’un trajet
- Top stations départ
- Top stations arrivée
- Nombre de stations visitées par vélo
- Vélos les plus utilisés (top N)

---

# 📊 **Schéma visuel du Pipeline (Image)**

Ajoutez l’image :

```md
![Pipeline](image/pipeline.png)


```md
![ERD](image/erd.png)
