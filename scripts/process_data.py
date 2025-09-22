import pandas as pd
import geopandas as gpd
from scipy.stats import pearsonr
import numpy as np

# Load raw
trees_df = pd.read_csv('data/raw_trees.csv')
green_spaces_df = pd.read_csv('data/raw_green_spaces.csv')
air_quality_df = pd.read_csv('data/raw_air_quality.csv')
cooling_spaces_df = pd.read_csv('data/raw_cooling_spaces.csv')
arr_df = pd.read_csv('data/raw_arrondissements.csv')
airparif_df = pd.read_csv('data/raw_airparif_measurements.csv')

# Mini-algo: Clean trees (handle geo_point_2d as [lat, lon])
trees_df['lat'] = trees_df['fields.geo_point_2d'].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) == 2 else np.nan)
trees_df['lon'] = trees_df['fields.geo_point_2d'].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) == 2 else np.nan)
trees_gdf = gpd.GeoDataFrame(trees_df.dropna(subset=['lat', 'lon']), geometry=gpd.points_from_xy(trees_df['lon'], trees_df['lat']), crs="EPSG:4326")

# Green spaces: Assume 'fields.geom' as WKT (adapt if XY)
green_spaces_gdf = gpd.GeoDataFrame(green_spaces_df, geometry=gpd.GeoSeries.from_wkt(green_spaces_df['fields.geom']), crs="EPSG:4326")
green_spaces_gdf['area_m2'] = green_spaces_gdf.geometry.area * 1e6  # Approx m²

# Arrondissements
arr_gdf = gpd.GeoDataFrame(arr_df, geometry=gpd.GeoSeries.from_wkt(arr_df['fields.geom']), crs="EPSG:4326")
arr_gdf['area_km2'] = arr_gdf.geometry.area / 1e6

# Cooling spaces (similar to trees)
cooling_spaces_gdf = gpd.GeoDataFrame(cooling_spaces_df, geometry=gpd.points_from_xy(cooling_spaces_df['fields.lon'], cooling_spaces_df['fields.lat']), crs="EPSG:4326")

# Airparif: Average pollutants
airparif_avg = airparif_df.groupby('id_site')[['no2', 'pm25']].mean().reset_index()  # Adapt fields

# Mini-algo: Spatial joins
trees_in_arr = gpd.sjoin(trees_gdf, arr_gdf[['geometry', 'c_ar']], how="left", predicate="within")
green_in_arr = gpd.sjoin(green_spaces_gdf, arr_gdf[['geometry', 'c_ar']], how="left", predicate="within")
cooling_in_arr = gpd.sjoin(cooling_spaces_gdf, arr_gdf[['geometry', 'c_ar']], how="left", predicate="within")

# Metrics
tree_density = trees_in_arr.groupby('c_ar').size().reset_index(name='tree_count')
tree_density = tree_density.merge(arr_gdf[['c_ar', 'area_km2']], on='c_ar')
tree_density['density_per_km2'] = tree_density['tree_count'] / tree_density['area_km2']

green_coverage = green_in_arr.groupby('c_ar')['area_m2'].sum().reset_index(name='green_area_m2')
green_coverage = green_coverage.merge(arr_gdf[['c_ar', 'area_km2']], on='c_ar')
green_coverage['coverage_per_km2'] = green_coverage['green_area_m2'] / (green_coverage['area_km2'] * 1e6)

cooling_count = cooling_in_arr.groupby('c_ar').size().reset_index(name='cooling_count')

# Merge & correlate (assume airparif spatial join similar)
merged = tree_density.merge(green_coverage, on='c_ar').merge(cooling_count, on='c_ar')
corr = pearsonr(merged['density_per_km2'].fillna(0), merged['no2'].fillna(0) if 'no2' in merged else [0]*len(merged))[0]  # Adapt
print(f"Tree density vs NO2 correlation: {corr}")

# Export
merged.to_csv('data/processed_analysis.csv', index=False)
merged.to_json('data/processed_analysis.json', orient='records')
print("Processing and export complete.")