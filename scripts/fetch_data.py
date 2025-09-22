import requests
import pandas as pd
from bs4 import BeautifulSoup
import io
import time  # For rate limiting best practice

# Mini-algo: API fetch with pagination and error fallback
def fetch_from_paris_api(dataset, rows=10000, delay=1):
    url = "https://opendata.paris.fr/api/records/1.0/search/"
    params = {"dataset": dataset, "rows": rows, "start": 0}
    all_records = []
    while True:
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_records.extend(data['records'])
            if len(data['records']) < rows:
                break
            params['start'] += rows
            time.sleep(delay)  # Best practice: Avoid rate limits
        except Exception as e:
            print(f"API error for {dataset}: {e}. Falling back to scraping.")
            return scrape_paris_csv(dataset)
    df = pd.json_normalize(all_records)
    return df

# Mini-algo: Scraping fallback - Download CSV from export page
def scrape_paris_csv(dataset):
    try:
        export_url = f"https://opendata.paris.fr/explore/dataset/{dataset}/export/"
        response = requests.get(export_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        csv_link = [a['href'] for a in soup.find_all('a', href=True) if 'csv' in a['href']][0]
        csv_response = requests.get(csv_link, timeout=30)
        csv_response.raise_for_status()
        # Parse CSV (Paris uses semicolon separator)
        df = pd.read_csv(io.StringIO(csv_response.text), sep=';')
        print(f"Scraped {len(df)} records for {dataset}.")
        return df
    except Exception as e:
        raise ValueError(f"Scraping failed for {dataset}: {e}")

# Dataset-specific fetches (API primary)
def fetch_trees():
    df = fetch_from_paris_api("les-arbres")
    df.to_csv('data/raw_trees.csv', index=False)
    return df

def fetch_green_spaces():
    df = fetch_from_paris_api("espaces-verts-et-assimiles")  # Exact slug from list
    df.to_csv('data/raw_green_spaces.csv', index=False)
    return df

def fetch_air_quality():
    df = fetch_from_paris_api("qualite-de-lair-concentration-moyenne-no2-pm2-5-pm10-o3-a-partir-de-2015")
    df.to_csv('data/raw_air_quality.csv', index=False)
    return df

def fetch_cooling_spaces():
    df = fetch_from_paris_api("ilots-de-fraicheur-espaces-verts-frais")
    df.to_csv('data/raw_cooling_spaces.csv', index=False)
    return df

def fetch_arrondissements():
    df = fetch_from_paris_api("arrondissements")
    df.to_csv('data/raw_arrondissements.csv', index=False)
    return df

# Airparif (complementary API; scraping fallback not needed as it's REST)
def fetch_airparif_measurements(year=2025):
    url = "https://services9.arcgis.com/7Sr9EkvgbJsCyFVQ/arcgis/rest/services/indice_atmo_agglo_paris/FeatureServer/0/query"
    params = {"where": f"date_ech >= '{year}-01-01'", "outFields": "*", "f": "geojson", "returnGeometry": "true"}
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize([f['properties'] for f in data['features']])
    except Exception as e:
        print(f"Airparif error: {e}. Use manual download.")
        df = pd.DataFrame()
    df.to_csv('data/raw_airparif_measurements.csv', index=False)
    return df

if __name__ == "__main__":
    fetch_trees()
    fetch_green_spaces()
    fetch_air_quality()
    fetch_cooling_spaces()
    fetch_arrondissements()
    fetch_airparif_measurements()
    print("All data fetched and exported to CSV.")