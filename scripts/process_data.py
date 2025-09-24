import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import logging
import json
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure output directory exists
os.makedirs('data/processed', exist_ok=True)

def process_trees():
    try:
        # Read trees data
        trees_df = pd.read_csv('data/raw_trees.csv', encoding='utf-8')
        logger.info(f"Trees columns: {trees_df.columns.tolist()}")
        
        # Read arrondissements data
        arr_df = pd.read_csv('data/raw_arrondissements.csv', encoding='utf-8')
        logger.info(f"Arrondissements columns: {arr_df.columns.tolist()}")
        
        # Normalize arrondissement names in trees_df
        def normalize_arrondissement(x):
            if pd.isna(x):
                return None
            x = str(x).replace('PARIS ', '').replace('E ARRDT', '').replace('ER ARRDT', '')
            if x.isdigit() and 1 <= int(x) <= 20:
                return f"750{x.zfill(2)}"
            return None
        
        trees_df['arrondissement'] = trees_df['arrondissement'].apply(normalize_arrondissement)
        invalid_arr = trees_df[trees_df['arrondissement'].isna()]['arrondissement'].unique()
        logger.info(f"Invalid arrondissements in trees: {invalid_arr}")
        
        # Filter out non-Paris arrondissements
        valid_arrondissements = [f"750{i:02d}" for i in range(1, 21)]
        trees_df = trees_df[trees_df['arrondissement'].isin(valid_arrondissements)]
        
        logger.info(f"Unique arrondissements in trees: {sorted(trees_df['arrondissement'].dropna().unique())}")
        logger.info(f"Unique arrondissements in arr_df: {sorted(arr_df['fields.c_ar'].unique())}")
        
        # Count trees per arrondissement
        tree_counts = trees_df.groupby('arrondissement').size().reset_index(name='tree_count')
        
        # Ensure all 20 arrondissements are included
        all_arrondissements = pd.DataFrame({'arrondissement': valid_arrondissements})
        tree_counts = all_arrondissements.merge(tree_counts, on='arrondissement', how='left').fillna({'tree_count': 0})
        
        logger.info(f"Tree counts by arrondissement: {tree_counts.to_dict()}")
        
        # Merge with arrondissement areas
        arr_df['arrondissement'] = arr_df['fields.c_ar'].apply(lambda x: f"750{x:02d}")
        arr_df['area_km2'] = arr_df['fields.surface'] / 1_000_000  # Convert m² to km²
        tree_density = tree_counts.merge(arr_df[['arrondissement', 'area_km2']], on='arrondissement', how='left')
        
        # Calculate tree density (trees per km²)
        tree_density['tree_density'] = tree_density['tree_count'] / tree_density['area_km2']
        
        # Save to CSV
        output_path = 'data/processed/processed_tree_density.csv'
        tree_density.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved tree density data to {output_path} ({len(tree_density)} records)")
        
        return tree_density
    except Exception as e:
        logger.error(f"Failed to process trees: {e}")
        return pd.DataFrame()

def process_green_spaces():
    try:
        # Read green spaces data
        green_df = pd.read_csv('data/raw_green_spaces.csv', encoding='utf-8')
        logger.info(f"Green spaces columns: {green_df.columns.tolist()}")
        
        # Extract postal code as arrondissement
        green_df['arrondissement'] = green_df['fields.adresse_codepostal'].astype(str).str[:5]
        logger.info(f"Unique arrondissements in green spaces: {green_df['arrondissement'].unique().tolist()}")
        
        # Filter for valid Paris arrondissements
        valid_arrondissements = [f"750{i:02d}" for i in range(1, 21)]
        green_df = green_df[green_df['arrondissement'].isin(valid_arrondissements)]
        logger.info(f"Valid green spaces after filtering: {len(green_df)}")
        
        # Convert to GeoDataFrame
        def parse_geometry(geom):
            try:
                if isinstance(geom, str):
                    geom = json.loads(geom)
                if not isinstance(geom, list) or len(geom) == 0:
                    raise ValueError("Empty or invalid geometry")
                
                # Helper function to validate coordinates
                def is_valid_coords(coords):
                    if not isinstance(coords, list) or len(coords) < 3:
                        return False
                    return all(
                        isinstance(coord, list) and len(coord) == 2 and all(isinstance(c, (int, float)) and not pd.isna(c) for c in coord)
                        for coord in coords
                    ) and len(set(tuple(coord) for coord in coords)) >= 3  # Ensure non-degenerate
                
                # Handle Polygon or MultiPolygon
                if is_valid_coords(geom):
                    return Polygon(geom)
                elif isinstance(geom, list) and all(isinstance(poly, list) for poly in geom):
                    valid_polys = [Polygon(poly) for poly in geom if is_valid_coords(poly)]
                    if len(valid_polys) == 1:
                        return valid_polys[0]
                    elif len(valid_polys) > 1:
                        return MultiPolygon(valid_polys)
                    raise ValueError("No valid polygons found")
                else:
                    raise ValueError(f"Invalid geometry structure: {str(geom)[:100]}")
            except Exception as e:
                logger.warning(f"Failed to parse geometry: {e}, value: {str(geom)[:100]}")
                return None
        
        green_df['geometry'] = green_df['fields.geom.coordinates'].apply(parse_geometry)
        green_df = green_df[green_df['geometry'].notnull()]  # Remove invalid geometries
        logger.info(f"Number of valid geometries: {len(green_df)}")
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(green_df, geometry='geometry', crs='EPSG:4326')
        
        # Re-project to EPSG:2154 (Lambert-93) for accurate area calculations
        gdf = gdf.to_crs(epsg=2154)
        gdf['area_m2'] = gdf['geometry'].area  # Area in square meters
        green_spaces = gdf.groupby('arrondissement')['area_m2'].sum().reset_index()
        green_spaces['area_km2'] = green_spaces['area_m2'] / 1_000_000
        
        # Ensure all 20 arrondissements
        all_arrondissements = pd.DataFrame({'arrondissement': valid_arrondissements})
        green_spaces = all_arrondissements.merge(green_spaces, on='arrondissement', how='left').fillna({'area_m2': 0, 'area_km2': 0})
        
        # Save to CSV
        output_path = 'data/processed/processed_green_spaces.csv'
        green_spaces.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved green spaces data to {output_path} ({len(green_spaces)} records)")
        
        return green_spaces
    except Exception as e:
        logger.error(f"Failed to process green spaces: {e}")
        return pd.DataFrame()

def process_air_quality():
    try:
        # Read air quality data
        if not os.path.exists('data/raw_air_quality.csv'):
            logger.error("File data/raw_air_quality.csv does not exist.")
            return pd.DataFrame()
        
        air_df = pd.read_csv('data/raw_air_quality.csv', encoding='utf-8')
        logger.info(f"Air quality columns: {air_df.columns.tolist()}")
        logger.info(f"Air quality sample: {air_df.head(3).to_dict()}")
        
        # Map air quality categories to approximate ATMO indices
        air_df['bonne_days'] = air_df['fields.ind_jour_qa_bonne'].fillna(0)
        air_df['moyenne_days'] = air_df['fields.ind_jour_qa_moyenne'].fillna(0)
        air_df['degradee_days'] = air_df['fields.ind_jour_qa_degradee'].fillna(0)
        air_df['mauvaise_days'] = air_df['fields.ind_jour_qa_mauvaise'].fillna(0)
        air_df['tres_mauvaise_days'] = air_df['fields.ind_jour_qa_tres_mauvaise'].fillna(0)
        air_df['extremement_mauvaise_days'] = air_df['fields.ind_jour_qa_extremement_mauvaise'].fillna(0)
        
        # Calculate weighted average ATMO index per year
        air_df['total_days'] = (
            air_df['bonne_days'] + air_df['moyenne_days'] + air_df['degradee_days'] +
            air_df['mauvaise_days'] + air_df['tres_mauvaise_days'] + air_df['extremement_mauvaise_days']
        )
        air_df['avg_code_qual'] = (
            (air_df['bonne_days'] * 1.5 +
             air_df['moyenne_days'] * 3.0 +
             air_df['degradee_days'] * 4.0 +
             air_df['mauvaise_days'] * 4.0 +
             air_df['tres_mauvaise_days'] * 5.0 +
             air_df['extremement_mauvaise_days'] * 5.0)
        ) / air_df['total_days']
        
        # Map avg_code_qual to lib_qual
        def map_lib_qual(code):
            if pd.isna(code):
                return 'Inconnu'
            if code <= 2:
                return 'Bon'
            elif code <= 3:
                return 'Moyen'
            elif code <= 4:
                return 'Dégradé'
            else:
                return 'Mauvais'
        
        air_df['lib_qual'] = air_df['avg_code_qual'].apply(map_lib_qual)
        
        # Select relevant columns
        air_quality = air_df[['fields.annee', 'avg_code_qual', 'lib_qual']].rename(columns={'fields.annee': 'year'})
        
        # Save to CSV
        output_path = 'data/processed/processed_air_quality.csv'
        air_quality.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved air quality data to {output_path} ({len(air_quality)} records)")
        
        return air_quality
    except Exception as e:
        logger.error(f"Failed to process air quality: {e}")
        return pd.DataFrame()

def process_cooling_spaces():
    try:
        # Read cooling spaces data (assuming JSON format matching the provided record)
        # If data is in CSV, adjust the loading mechanism accordingly
        try:
            with open('data/raw_cooling_spaces.json', 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            logger.warning("data/raw_cooling_spaces.json not found. Attempting to read data/raw_cooling_spaces.csv")
            cool_df = pd.read_csv('data/raw_cooling_spaces.csv', encoding='utf-8')
            # Convert CSV to JSON-like structure if necessary
            raw_data = cool_df.to_dict('records')
        
        logger.info(f"Cooling spaces data loaded: {len(raw_data)} records")
        
        # Convert to DataFrame
        cool_df = pd.DataFrame(raw_data)
        logger.info(f"Cooling spaces columns: {cool_df.columns.tolist()}")
        
        # Normalize arrondissement
        def normalize_cooling_arrondissement(x):
            if pd.isna(x):
                logger.warning(f"Missing arrondissement value in record")
                return None
            try:
                x = str(x).strip()
                if x.startswith('750') and x[3:5].isdigit() and 1 <= int(x[3:5]) <= 20:
                    return x[:5]
                # Handle integer or float values (e.g., 75017.0)
                x_int = int(float(x))
                if 75001 <= x_int <= 75020:
                    return f"{x_int:05d}"
                logger.warning(f"Invalid arrondissement value: {x}")
                return None
            except (ValueError, TypeError):
                logger.warning(f"Invalid arrondissement value: {x}")
                return None
        
        # Apply normalization to the 'fields.arrondissement' column
        if 'fields.arrondissement' in cool_df.columns:
            cool_df['arrondissement'] = cool_df['fields.arrondissement'].apply(normalize_cooling_arrondissement)
        else:
            logger.error("Column 'fields.arrondissement' not found in cooling spaces data")
            return pd.DataFrame()
        
        # Filter for valid Paris arrondissements
        valid_arrondissements = [f"750{i:02d}" for i in range(1, 21)]
        cool_df = cool_df[cool_df['arrondissement'].isin(valid_arrondissements)]
        logger.info(f"Valid cooling spaces after filtering: {len(cool_df)}")
        
        # Count cooling spaces per arrondissement
        cooling_counts = cool_df.groupby('arrondissement').size().reset_index(name='cooling_space_count')
        
        # Ensure all 20 arrondissements are included
        all_arrondissements = pd.DataFrame({'arrondissement': valid_arrondissements})
        cooling_spaces = all_arrondissements.merge(cooling_counts, on='arrondissement', how='left').fillna({'cooling_space_count': 0})
        
        # Convert counts to integers
        cooling_spaces['cooling_space_count'] = cooling_spaces['cooling_space_count'].astype(int)
        
        # Log non-zero counts to verify
        non_zero_counts = cooling_spaces[cooling_spaces['cooling_space_count'] > 0][['arrondissement', 'cooling_space_count']]
        logger.info(f"Arrondissements with cooling spaces: {non_zero_counts.to_dict('records')}")
        
        # Save to CSV
        output_path = 'data/processed/processed_cooling_spaces.csv'
        cooling_spaces.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved cooling spaces data to {output_path} ({len(cooling_spaces)} records)")
        
        return cooling_spaces
    except Exception as e:
        logger.error(f"Failed to process cooling spaces: {e}")
        return pd.DataFrame()

def process_arrondissements():
    try:
        # Read arrondissements data
        arr_df = pd.read_csv('data/raw_arrondissements.csv', encoding='utf-8')
        logger.info(f"Arrondissements columns: {arr_df.columns.tolist()}")
        
        # Normalize arrondissement codes
        arr_df['arrondissement'] = arr_df['fields.c_ar'].apply(lambda x: f"750{x:02d}")
        arr_df['area_km2'] = arr_df['fields.surface'] / 1_000_000  # Convert m² to km²
        arr_df['name'] = arr_df['fields.l_aroff'].str.title()  # Official arrondissement name
        
        # Parse geometry
        def parse_geometry(geom):
            try:
                if isinstance(geom, str):
                    geom = json.loads(geom)
                if not isinstance(geom, list) or len(geom) == 0:
                    raise ValueError("Empty or invalid geometry")
                
                # Helper function to validate coordinates
                def is_valid_coords(coords):
                    if not isinstance(coords, list) or len(coords) < 3:
                        return False
                    return all(
                        isinstance(coord, list) and len(coord) == 2 and all(isinstance(c, (int, float)) and not pd.isna(c) for c in coord)
                        for coord in coords
                    ) and len(set(tuple(coord) for coord in coords)) >= 3  # Ensure non-degenerate
                
                # Handle Polygon or MultiPolygon
                if is_valid_coords(geom):
                    return Polygon(geom)
                elif isinstance(geom, list) and all(isinstance(poly, list) for poly in geom):
                    valid_polys = [Polygon(poly) for poly in geom if is_valid_coords(poly)]
                    if len(valid_polys) == 1:
                        return valid_polys[0]
                    elif len(valid_polys) > 1:
                        return MultiPolygon(valid_polys)
                    raise ValueError("No valid polygons found")
                else:
                    raise ValueError(f"Invalid geometry structure: {str(geom)[:100]}")
            except Exception as e:
                logger.warning(f"Failed to parse arrondissement geometry: {e}, value: {str(geom)[:100]}")
                return None
        
        arr_df['geometry'] = arr_df['fields.geom.coordinates'].apply(parse_geometry)
        arr_df = arr_df[arr_df['geometry'].notnull()]
        logger.info(f"Number of valid arrondissement geometries: {len(arr_df)}")
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(arr_df, geometry='geometry', crs='EPSG:4326')
        
        # Select relevant columns
        gdf = gdf[['arrondissement', 'name', 'area_km2', 'geometry']]
        
        # Convert geometry to WKT for CSV
        gdf.loc[:, 'geometry'] = gdf['geometry'].apply(lambda x: x.wkt if x is not None else '')
        
        # Save to CSV
        output_path = 'data/processed/processed_arrondissements.csv'
        gdf.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Saved arrondissements data to {output_path} ({len(gdf)} records)")
        
        return gdf
    except Exception as e:
        logger.error(f"Failed to process arrondissements: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    process_trees()
    process_green_spaces()
    process_air_quality()
    cooling_spaces = process_cooling_spaces()
    process_arrondissements()
    logger.info("All data processed and saved to data/processed/")
    # Log final cooling spaces output for verification
    logger.info(f"Final cooling spaces counts: {cooling_spaces[['arrondissement', 'cooling_space_count']].to_dict('records')}")