import pandas as pd
from sqlalchemy import create_engine
import os

DB_URL = os.getenv('DATABASE_URL', 'postgresql://user:pass@localhost:5432/paris_data')
engine = create_engine(DB_URL)

# Enable PostGIS (run manually once: psql -d paris_data -c "CREATE EXTENSION postgis;")
pd.read_csv('data/processed_analysis.csv').to_sql('analysis_results', engine, if_exists='replace', index=False)
pd.read_csv('data/raw_trees.csv').head(10000).to_sql('trees', engine, if_exists='replace', index=False)  # Subset
print("Data loaded to DB.")