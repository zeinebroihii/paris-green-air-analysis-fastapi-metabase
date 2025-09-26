import pandas as pd
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, text
from geoalchemy2 import Geometry
from shapely import wkt
from shapely.geometry import Polygon
import logging
import os
import io
import json
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
DB_PARAMS = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# File paths
FILE_PATHS = {
    'air_quality': 'data/processed/processed_air_quality.csv',
    'arrondissements': 'data/processed/processed_arrondissements.csv',
    'green_spaces': 'data/processed/processed_green_spaces.csv',
    'tree_density': 'data/processed/processed_tree_density.csv',
    'cooling_spaces_counts': 'data/processed/processed_cooling_spaces.csv'
}

os.makedirs('data/processed', exist_ok=True)

def list_tables(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            tables = [row[0] for row in result]
            logger.info(f"Tables in public schema: {tables}")
            return tables
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        return []

def get_table_columns(engine, table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT column_name FROM information_schema.columns "
                     f"WHERE table_schema = 'public' AND table_name = '{table_name}'"))
            columns = [row[0] for row in result]
            logger.info(f"Columns in public.{table_name}: {columns}")
            return columns
    except Exception as e:
        logger.error(f"Failed to get columns for public.{table_name}: {e}")
        return []

def infer_column_type(series, col_name, table_name):
    dtype = str(series.dtype)
    if col_name in ['geometry', 'geom']:
        return Geometry('GEOMETRY', srid=4326)
    if 'int' in dtype:
        return Integer
    elif 'float' in dtype:
        return Float
    elif 'object' in dtype or 'string' in dtype:
        return String(255) if not (table_name == 'cooling_spaces_counts' and col_name == 'adresse') else String(1000)
    return String(255)

def create_dynamic_table(engine, table_name, df, primary_key=None, geometry_columns=None):
    metadata = MetaData()
    columns = []
    for col in df.columns:
        col_type = infer_column_type(df[col], col, table_name)
        columns.append(Column(col, col_type, primary_key=(col == primary_key)))
    table = Table(table_name, metadata, *columns, schema='public')
    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            table.drop(conn, checkfirst=True)
            table.create(conn)
            conn.commit()
            logger.info(f"Created table public.{table_name} with columns: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Failed to create table public.{table_name}: {e}")
            raise
    return table

def normalize_arrondissement(x):
    if pd.isna(x):
        logger.warning("Missing arrondissement value")
        return None
    try:
        x = str(x).strip()
        if x.startswith('750') and x[3:5].isdigit() and 1 <= int(x[3:5]) <= 20:
            return x[:5]
        x_int = int(float(x))
        if 75001 <= x_int <= 75020:
            return f"{x_int:05d}"
        logger.warning(f"Invalid arrondissement value: {x}")
        return None
    except (ValueError, TypeError):
        logger.warning(f"Invalid arrondissement value: {x}")
        return None

def safe_shape(x):
    if pd.isna(x):
        return None
    try:
        geom = wkt.loads(x)
        return geom.wkt
    except Exception:
        try:
            parsed = json.loads(x)
            from shapely.geometry import shape
            return shape(parsed).wkt
        except (json.JSONDecodeError, ValueError, TypeError):
            try:
                coords = [tuple(map(float, pair.strip().split())) for pair in x.split(',') if pair.strip()]
                if coords:
                    polygon = Polygon(coords)
                    return polygon.wkt
                logger.warning(f"Invalid coordinate data: {x}")
                return None
            except Exception as e:
                logger.warning(f"Invalid geometry data: {x}, error: {e}")
                return None

def load_data(engine, table_name, csv_path, geometry_columns=None, use_copy=True, truncate=False):
    try:
        if not os.path.exists(csv_path):
            logger.error(f"File not found: {csv_path}")
            return

        df = pd.read_csv(csv_path, encoding='utf-8', sep=',')
        logger.info(f"Loaded {len(df)} records from {csv_path}")
        df.columns = [col.strip().replace(' ', '_') for col in df.columns]
        logger.info(f"Cleaned column names: {df.columns.tolist()}")

        if table_name == 'cooling_spaces_counts':
            expected_columns = ['arrondissement', 'cooling_space_count']
            if not all(col in df.columns for col in expected_columns):
                logger.error(f"Expected columns {expected_columns} not found in {csv_path}: {df.columns.tolist()}")
                return
            df['arrondissement'] = df['arrondissement'].apply(normalize_arrondissement)
            df = df[df['arrondissement'].notnull()]
            logger.info(f"Valid records after arrondissement filtering: {len(df)}")
        elif table_name == 'air_quality':
            expected_columns = ['year', 'avg_code_qual', 'lib_qual']
            if not all(col in df.columns for col in expected_columns):
                logger.error(f"Expected columns {expected_columns} not found in {csv_path}: {df.columns.tolist()}")
                return
        elif table_name in ['green_spaces', 'tree_density']:
            expected_columns = {
                'green_spaces': ['arrondissement', 'area_m2', 'area_km2'],
                'tree_density': ['arrondissement', 'tree_count', 'area_km2', 'tree_density']
            }
            if not all(col in df.columns for col in expected_columns[table_name]):
                logger.error(f"Expected columns {expected_columns[table_name]} not found in {csv_path}: {df.columns.tolist()}")
                return

        if 'arrondissement' in df.columns:
            df['arrondissement'] = df['arrondissement'].apply(normalize_arrondissement)
            df = df[df['arrondissement'].notnull()]
            logger.info(f"Valid records after arrondissement filtering: {len(df)}")

        if geometry_columns:
            for col in geometry_columns:
                if col in df.columns:
                    df[col] = df[col].apply(safe_shape)
                    df = df[df[col].notnull()]
                    logger.info(f"Valid geometries for {col} in {table_name}: {len(df)}")

        # Ensure table exists or recreate if columns differ
        existing_columns = get_table_columns(engine, table_name)
        if set(df.columns) != set(existing_columns):
            logger.warning(f"Column mismatch for {table_name}, recreating table")
            create_dynamic_table(engine, table_name, df, 'arrondissement' if table_name == 'cooling_spaces_counts' else None, geometry_columns)

        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables "
                                       f"WHERE table_schema = 'public' AND table_name = '{table_name}')"))
            if not result.fetchone()[0]:
                raise Exception(f"Table {table_name} does not exist")

        if use_copy:
            conn = engine.raw_connection()
            try:
                cursor = conn.cursor()
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE public.{table_name}")
                    conn.commit()
                output = io.StringIO()
                df.to_csv(output, index=False, sep='|', na_rep='')
                output.seek(0)
                copy_sql = f"COPY public.{table_name} ({','.join(df.columns)}) FROM STDIN WITH CSV HEADER DELIMITER '|' NULL ''"
                cursor.copy_expert(copy_sql, output)
                conn.commit()
                logger.info(f"Loaded {len(df)} records into {table_name} using COPY")
            except Exception as e:
                logger.error(f"Failed COPY for {table_name}: {e}")
                df.to_sql(table_name, engine, if_exists='append', index=False, schema='public',
                          dtype={col: Geometry('GEOMETRY', srid=4326) for col in geometry_columns or []})
            finally:
                conn.close()
        else:
            df.to_sql(table_name, engine, if_exists='append', index=False, schema='public',
                      dtype={col: Geometry('GEOMETRY', srid=4326) for col in geometry_columns or []})

    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        raise

def main():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info(f"Database connection successful: {result.fetchone()}")
        list_tables(engine)

        table_configs = {
            'air_quality': {'csv': FILE_PATHS['air_quality'], 'primary_key': 'year', 'truncate': True},
            'arrondissements': {'csv': FILE_PATHS['arrondissements'], 'primary_key': 'arrondissement', 'geometry_columns': ['geometry']},
            'green_spaces': {'csv': FILE_PATHS['green_spaces'], 'primary_key': 'arrondissement', 'truncate': True},
            'tree_density': {'csv': FILE_PATHS['tree_density'], 'primary_key': 'arrondissement'},
            'cooling_spaces_counts': {'csv': FILE_PATHS['cooling_spaces_counts'], 'primary_key': 'arrondissement', 'truncate': True}
        }

        for table_name, config in table_configs.items():
            logger.info(f"Processing table: {table_name}")
            df = pd.read_csv(config['csv'], encoding='utf-8', sep=',')
            df.columns = [col.strip().replace(' ', '_') for col in df.columns]
            create_dynamic_table(engine, table_name, df, config.get('primary_key'), config.get('geometry_columns'))
            load_data(engine, table_name, config['csv'], config.get('geometry_columns'), use_copy=True, truncate=config.get('truncate', False))

        list_tables(engine)
        logger.info("All data loaded successfully into paris_environment database")
    except Exception as e:
        logger.error(f"Main process failed: {e}")
        raise

if __name__ == "__main__":
    main()