# app/etl.py
import os
import json
import gzip
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm
import datetime as dt

# --- 1. Charger les variables d'environnement ---
load_dotenv()  # Charge les variables du fichier .env

# --- 2. Configuration des variables ---
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DATA_FOLDER = os.getenv("DATA_FOLDER")
PROGRESS_FILE = os.getenv("PROGRESS_FILE", "progress.json")

# --- 3. Configuration des logs ---
logging.basicConfig(filename="data_import_errors.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 4. Fonction de connexion à la base de données ---
def get_db_connection():
    """
    Retourne une connexion psycopg2 à la base de données PostgreSQL.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

# --- 5. Gestion de la persistance du progrès ---
def load_progress():
    """
    Charge le fichier progress.json. Si ce fichier n'existe pas ou est vide,
    il essaie de le reconstruire automatiquement.
    """
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                # Vérification que le fichier n'est pas vide
                content = f.read().strip()
                if not content:
                    print("⚠️ progress.json est vide — reconstruction automatique…")
                    return rebuild_progress_from_database()
                return json.loads(content)
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️ Erreur de lecture de progress.json: {e} — reconstruction automatique…")
            return rebuild_progress_from_database()
    else:
        print("⚠️ progress.json introuvable — reconstruction automatique…")
        return rebuild_progress_from_database()


def save_progress(progress):
    """
    Sauvegarde l'état actuel du progrès dans le fichier progress.json.
    """
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=4)

def rebuild_progress_from_database():
    """
    Reconstruit le fichier progress.json à partir des snapshots existants en base de données.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Lire tous les timestamps déjà importés
    cursor.execute("SELECT timestamp_capture FROM snapshot;")
    existing_snapshots = {row[0] for row in cursor.fetchall()}

    cursor.close()
    conn.close()

    print(f"   → {len(existing_snapshots)} snapshots trouvés en base.")

    # 2. Récupérer tous les fichiers disponibles dans le dossier
    all_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(('.json', '.gz'))]

    done_files = []

    # 3. Vérifier si le fichier a déjà été traité
    for filename in all_files:
        timestamp = parse_timestamp_from_filename(filename)
        if timestamp in existing_snapshots:
            done_files.append(filename)

    print(f"   → {len(done_files)} fichiers déjà traités détectés.")

    return {"done": done_files}

# --- 6. Parser le timestamp depuis le nom du fichier ---
def parse_timestamp_from_filename(filename: str) -> dt.datetime:
    try:
        date_time_str = filename.split('_')[1] + '_' + filename.split('_')[2]
        return dt.datetime.strptime(date_time_str, '%Y%m%d_%H%M%S')
    except:
        return None

# --- 7. Fonction d'insertion dans la base de données ---
def insert_snapshot_and_get_id(cursor, timestamp: dt.datetime):
    """
    Insère un snapshot et retourne son ID.
    """
    try:
        cursor.execute(
            """
            INSERT INTO snapshot (timestamp_capture)
            VALUES (%s)
            ON CONFLICT (timestamp_capture) DO UPDATE SET timestamp_capture = EXCLUDED.timestamp_capture
            RETURNING snapshot_id;
            """,
            (timestamp,)
        )
        return cursor.fetchone()[0]
    except Exception as e:
        cursor.connection.rollback()
        logging.error(f"Error inserting snapshot: {e}")
        return None

def process_file_content(filename, content, timestamp, cursor, errors):
    """
    Traite le contenu d'un fichier JSON et l'insère dans la base de données.
    """
    snapshot_id = insert_snapshot_and_get_id(cursor, timestamp)
    if snapshot_id is None:
        errors['snapshot_error'] += 1
        return

    try:
        data = json.loads(content)
        if not isinstance(data, list):
            errors['invalid_format'] += 1
            return

        station_buffer = []
        velo_buffer = []
        etat_station_buffer = []
        localisation_velo_buffer = []

        for station_data in data:
            station_info = station_data.get('station', {})
            station_code = station_info.get('code')
            if not station_code:
                continue

            station_buffer.append((
                station_code,
                station_info.get('name'),
                station_info.get('gps', {}).get('latitude'),
                station_info.get('gps', {}).get('longitude'),
                station_info.get('stationType'),
                station_info.get('type')
            ))

            etat_station_buffer.append((
                snapshot_id,
                station_code,
                station_data.get('state'),
                station_data.get('nbBike'),
                station_data.get('nbEbike'),
                station_data.get('nbFreeDock')
            ))

            for bike in station_data.get('bikes', []):
                velo_name = bike.get('bikeName')
                if not velo_name:
                    continue

                velo_buffer.append((velo_name, bike.get('bikeElectric')))
                localisation_velo_buffer.append((
                    snapshot_id,
                    velo_name,
                    station_code,
                    bike.get('bikeStatus'),
                    bike.get('dockPosition')
                ))

        # Insertion des données dans la base de données
        if station_buffer:
            execute_values(cursor, """
                INSERT INTO station (station_code, name, latitude, longitude, stationtype, type)
                VALUES %s
                ON CONFLICT (station_code) DO NOTHING
            """, station_buffer)

        if velo_buffer:
            execute_values(cursor, """
                INSERT INTO velo (velo_name, bikeelectric)
                VALUES %s
                ON CONFLICT (velo_name) DO NOTHING
            """, velo_buffer)

        if etat_station_buffer:
            execute_values(cursor, """
                INSERT INTO etat_station (snapshot_id, station_code, state, nbbike, nbebike, nbfreedock)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, etat_station_buffer)

        if localisation_velo_buffer:
            execute_values(cursor, """
                INSERT INTO localisation_velo (snapshot_id, velo_name, station_code, bikestatus, dockposition)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, localisation_velo_buffer)

        cursor.connection.commit()

    except Exception as e:
        cursor.connection.rollback()
        logging.error(f"Processing error {filename}: {e}")
        errors['generic_error'] += 1

# --- 8. Fonction principale pour l'importation des données ---
def import_data_from_folder(data_folder):
    """
    Processus d'importation des fichiers dans le dossier.
    """
    progress = load_progress()
    done_files = set(progress["done"])

    conn = get_db_connection()
    cursor = conn.cursor()

    all_files = [f for f in os.listdir(data_folder) if f.endswith(('.json', '.gz'))]
    remaining_files = [f for f in all_files if f not in done_files]

    errors = {'invalid_format': 0, 'snapshot_error': 0, 'generic_error': 0}

    with tqdm(total=len(all_files), desc="Processing", ncols=100) as pbar:

        pbar.update(len(done_files))

        for filename in remaining_files:
            filepath = os.path.join(data_folder, filename)
            timestamp = parse_timestamp_from_filename(filename)

            if timestamp is None:
                errors['generic_error'] += 1
                progress["done"].append(filename)
                save_progress(progress)
                pbar.update(1)
                continue

            try:
                # Lecture du fichier
                if filename.endswith('.gz'):
                    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                        content = f.read()
                else:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read()

                process_file_content(filename, content, timestamp, cursor, errors)

                # Sauvegarde de la progression
                progress["done"].append(filename)
                save_progress(progress)

            except Exception as e:
                logging.error(f"Failed to read {filename}: {e}")
                errors['generic_error'] += 1

            pbar.set_postfix(errors=errors, refresh=True)
            pbar.update(1)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    import_data_from_folder(DATA_FOLDER)
