import requests
import pandas as pd
import time
import os
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import io

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Cache file for les-arbres progress
CACHE_FILE = 'data/les_arbres_progress.json'

# Single chunk fetch via API
def fetch_chunk(dataset, start, rows, session):
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {"dataset": dataset, "rows": rows, "start": start}
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data['records']
        logger.info(f"Fetched {len(records)} records for {dataset} at start={start}")
        return records, None
    except Exception as e:
        logger.error(f"Chunk failed for {dataset} at start={start}: {e} - {response.text if 'response' in locals() else 'No response'}")
        return [], str(e)

# Fetch full dataset via CSV download
def fetch_csv_download(dataset, save_as):
    csv_url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv?lang=fr&timezone=Europe%2FParis"
    try:
        response = requests.get(csv_url, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), sep=';')
        df.to_csv(save_as, index=False)
        logger.info(f"Fetched {len(df)} records for {dataset} via CSV download to {save_as}")
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            logger.info(f"Deleted {CACHE_FILE}")
        return df
    except Exception as e:
        logger.error(f"CSV download failed for {dataset}: {e}")
        return pd.DataFrame()

# General API fetch helper
def fetch_from_paris_api(dataset, save_as, rows=1000, facets=None, parallel=False):
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    session = requests.Session()
    params = {"dataset": dataset, "rows": 0, "start": 0}
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        nhits = data['nhits']
        logger.info(f"Dataset {dataset} has {nhits} records.")
    except Exception as e:
        logger.error(f"Failed to get nhits for {dataset}: {e}. Attempting CSV download.")
        session.close()
        return fetch_csv_download(dataset, save_as)

    if dataset == "les-arbres" and nhits > 9000:
        logger.info(f"Switching to CSV download for {dataset} due to large dataset ({nhits} records)")
        session.close()
        return fetch_csv_download(dataset, save_as)

    if dataset == "les-arbres":
        cache = {'start': 0}
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                logger.info(f"Resuming les-arbres from start={cache['start']}")
            except Exception as e:
                logger.error(f"Failed to read {CACHE_FILE}: {e}. Starting from scratch.")
                cache = {'start': 0}
        start = cache['start']
        all_records = []
        if start > 0 and os.path.exists(save_as):
            all_records = pd.read_csv(save_as).to_dict('records')
            logger.info(f"Loaded {len(all_records)} records from existing {save_as}")
    else:
        all_records = []
        start = 0
        if facets:
            params["facet"] = facets
        params['rows'] = rows

    session_count = start
    while start < nhits:
        if session_count >= 5000:
            session.close()
            session = requests.Session()
            logger.info(f"Reset session for {dataset} at start={start}")
            session_count = 0

        if dataset == "les-arbres" and parallel:
            chunk_size = rows * 2
            starts = list(range(start, min(start + chunk_size, nhits), rows))
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(fetch_chunk, dataset, s, rows, session) for s in starts]
                chunk_records = []
                any_error = False
                for future in futures:
                    records, error = future.result()
                    if error:
                        any_error = True
                        logger.error(f"Chunk error at start={start}: {error}")
                    chunk_records.extend(records)
                if any_error:
                    logger.info(f"Switching to CSV download for {dataset} due to chunk errors")
                    session.close()
                    return fetch_csv_download(dataset, save_as)
                all_records.extend(chunk_records)
                session_count += len(chunk_records)
                start += chunk_size
                if not chunk_records:
                    logger.error(f"No records fetched for chunk at start={start}. Switching to CSV download.")
                    if all_records:
                        df = pd.json_normalize(all_records)
                        df.to_csv(save_as, index=False)
                        with open(CACHE_FILE, 'w') as f:
                            json.dump({'start': start}, f)
                        logger.warning(f"Partial data ({len(df)} records) saved to {save_as}")
                    session.close()
                    return fetch_csv_download(dataset, save_as)
        else:
            params['start'] = start
            params['rows'] = min(rows, nhits - start)
            for attempt in range(5):
                try:
                    response = session.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    records = data['records']
                    logger.info(f"Fetched {len(records)} records for {dataset} at start={start}")
                    all_records.extend(records)
                    start += params['rows']
                    session_count += params['rows']
                    time.sleep(0.2)
                    break
                except Exception as e:
                    logger.error(f"API fetch failed for {dataset} at start={start} (attempt {attempt+1}/5): {e}")
                    if attempt == 4:
                        logger.info(f"API failed after 5 attempts for {dataset}. Switching to CSV download.")
                        if all_records:
                            df = pd.json_normalize(all_records)
                            df.to_csv(save_as, index=False)
                            if dataset == "les-arbres":
                                with open(CACHE_FILE, 'w') as f:
                                    json.dump({'start': start}, f)
                            logger.warning(f"Partial data ({len(df)} records) saved to {save_as}")
                        session.close()
                        return fetch_csv_download(dataset, save_as)
                    time.sleep(2)
            if len(records) < params['rows']:
                break

        if dataset == "les-arbres" and len(all_records) >= 5000:
            df = pd.json_normalize(all_records)
            df.to_csv(save_as, index=False)
            with open(CACHE_FILE, 'w') as f:
                json.dump({'start': start}, f)
            logger.info(f"Incremental save: {len(df)} records to {save_as}")

    df = pd.json_normalize(all_records)
    df.to_csv(save_as, index=False)
    if dataset == "les-arbres" and os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        logger.info(f"Deleted {CACHE_FILE}")
    logger.info(f"Fetched {len(df)} records into {save_as} via API.")
    session.close()
    if len(df) < nhits:
        logger.info(f"API fetched only {len(df)} of {nhits} records. Fetching remaining via CSV download.")
        return fetch_csv_download(dataset, save_as)
    return df

# Dataset-specific fetches
def fetch_trees():
    return fetch_from_paris_api(
        "les-arbres",
        "data/raw_trees.csv",
        rows=1000,
        parallel=True
    )

def fetch_green_spaces():
    return fetch_from_paris_api(
        "espaces_verts",
        "data/raw_green_spaces.csv",
        rows=1000,
        facets=["type_espace", "arrondissement"]
    )

def fetch_air_quality():
    return fetch_from_paris_api(
        "qualite-de-l-air-indice-atmo",
        "data/raw_air_quality.csv",
        rows=1000,
        facets=["indice", "date_ech"]
    )

def fetch_cooling_spaces():
    facets = ["arrondissement", "ouvert_24h", "horaires_periode", "statut_ouverture", "canicule_ouverture", "ouverture_estivale_nocturne", "type"]
    return fetch_from_paris_api(
        "ilots-de-fraicheur-espaces-verts-frais",
        "data/raw_cooling_spaces.csv",
        rows=1000,
        facets=facets
    )

def fetch_arrondissements():
    return fetch_from_paris_api(
        "arrondissements",
        "data/raw_arrondissements.csv",
        rows=100,
        facets=["c_ar"]
    )

if __name__ == "__main__":
    fetch_trees()
    fetch_green_spaces()
    fetch_air_quality()
    fetch_cooling_spaces()
    fetch_arrondissements()
    logger.info("All datasets fetched and exported to CSV.")