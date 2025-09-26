from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import io
import base64
from matplotlib.patches import Circle
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap

# Setup logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DB_PARAMS = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

if not DB_PARAMS['password']:
    raise ValueError("POSTGRES_PASSWORD must be set in .env file")

# Initialize connection pool
db_pool = SimpleConnectionPool(1, 10, **DB_PARAMS)

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/images", StaticFiles(directory="images"), name="images")

def get_db_conn():
    try:
        conn = db_pool.getconn()
        return conn
    except Exception as e:
        logger.error(f"Failed to get DB connection: {e}")
        raise

def release_db_conn(conn):
    db_pool.putconn(conn)

def create_stunning_chart(data, chart_type, title, x_label, y_label, filename, **kwargs):
    """Create visually stunning charts with modern aesthetics"""
    
    # Set up the figure with dark theme
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(16, 10), facecolor='#0a0a0a')
    ax.set_facecolor('#111111')
    
    # Custom color palettes
    neon_colors = ['#00ff9f', '#00d4ff', '#ff6b35', '#f72585', '#7209b7', '#560bad']
    gradient_colors = ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#00f2fe']
    
    if chart_type == "enhanced_bar":
        bars = ax.bar(data[x_label], data[y_label], 
                     color=gradient_colors[:len(data)],
                     edgecolor='white', linewidth=2,
                     alpha=0.9, width=0.7)
        
        for i, bar in enumerate(bars):
            height = bar.get_height()
            # Add text with glow effect
            ax.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                   f'{height:.1f}', ha='center', va='bottom',
                   fontsize=14, fontweight='bold', color='white',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor=gradient_colors[i % len(gradient_colors)], alpha=0.7))
                     
    elif chart_type == "neon_scatter":
        scatter = ax.scatter(data[x_label], data[y_label], 
                           s=400, alpha=0.8, 
                           c=data[y_label] if len(data) > 1 else neon_colors[0],
                           cmap='plasma', edgecolors='white', linewidth=2)
        
        # Add connecting lines for flow effect
        if len(data) > 1:
            sorted_data = data.sort_values(x_label)
            ax.plot(sorted_data[x_label], sorted_data[y_label], 
                   color='#00ff9f', alpha=0.5, linewidth=3, linestyle='--')
        
        # Add colorbar with custom styling
        cbar = plt.colorbar(scatter, ax=ax, shrink=0.8)
        cbar.ax.yaxis.set_tick_params(color='white')
        cbar.outline.set_edgecolor('white')
        
    elif chart_type == "cyberpunk_line":
        ax.plot(data[x_label], data[y_label], 
               color='#00ff9f', linewidth=4, marker='o', 
               markersize=12, markerfacecolor='#ff6b35', 
               markeredgecolor='white', markeredgewidth=3)
        
        # Add area under curve with gradient
        ax.fill_between(data[x_label], data[y_label], alpha=0.3, 
                       color='#00ff9f')
        
        # Add average line
        avg_val = data[y_label].mean()
        ax.axhline(y=avg_val, color='#f72585', linestyle='--', 
                  linewidth=3, alpha=0.8, label=f'Average: {avg_val:.2f}')
    
    elif chart_type == "holographic_radar":
        # For radar chart (requires different approach)
        angles = np.linspace(0, 2 * np.pi, len(data), endpoint=False).tolist()
        values = data[y_label].tolist()
        values += values[:1]  # Complete the circle
        angles += angles[:1]
        
        ax = plt.subplot(111, projection='polar', facecolor='#111111')
        ax.plot(angles, values, 'o-', linewidth=3, color='#00ff9f', markersize=8)
        ax.fill(angles, values, alpha=0.25, color='#00d4ff')
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(data[x_label], color='white', fontsize=12)
        ax.grid(True, color='white', alpha=0.3)
    
    # Enhanced styling for all chart types
    if chart_type != "holographic_radar":
        # Gradient background
        gradient = np.linspace(0, 1, 256).reshape(256, -1)
        gradient = np.vstack((gradient, gradient))
        ax.imshow(gradient.T, extent=[ax.get_xlim()[0], ax.get_xlim()[1], 
                                     ax.get_ylim()[0], ax.get_ylim()[1]], 
                 aspect='auto', alpha=0.1, cmap='plasma')
    
    # Title with futuristic styling
    plt.suptitle(title, fontsize=24, fontweight='bold', color='white',
                y=0.95, bbox=dict(boxstyle="round,pad=0.5", 
                                 facecolor='#1a1a1a', alpha=0.8,
                                 edgecolor='#00ff9f', linewidth=2))
    
    # Axis labels with neon effect
    if chart_type != "holographic_radar":
        ax.set_xlabel(x_label, fontsize=16, color='#00d4ff', fontweight='bold')
        ax.set_ylabel(y_label, fontsize=16, color='#00d4ff', fontweight='bold')
        
        # Custom grid
        ax.grid(True, linestyle=':', alpha=0.4, color='white')
        ax.set_facecolor('#0a0a0a')
        
        # Tick styling
        ax.tick_params(colors='white', labelsize=12)
        for spine in ax.spines.values():
            spine.set_color('#00ff9f')
            spine.set_linewidth(2)
    
    plt.tight_layout()
    plt.savefig(f"images/{filename}.png", dpi=300, bbox_inches='tight', 
                facecolor='#0a0a0a', edgecolor='none')
    plt.close()

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("static/index.html", "r") as f:
        html_content = f.read()
    
    stats = await get_stats()
    
    # Replace placeholders with actual data
    replacements = [
        (stats.get('avg_air_quality', 0), "{:.2f}"),
        (stats.get('total_green_area', 0), "{:.2f}"),
        (stats.get('avg_tree_density', 0), "{:.2f}"),
        (stats.get('max_cooling_spaces_arr', 'N/A'), "{}"),
        (stats.get('avg_area_km2', 0), "{:.2f}")
    ]
    
    for value, format_str in replacements:
        if isinstance(value, (int, float)) and format_str != "{}":
            replacement = format_str.format(value)
        else:
            replacement = str(value)
        html_content = html_content.replace("[Loading...]", replacement, 1)
    
    return HTMLResponse(content=html_content)

async def get_stats():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get air quality stats
        cursor.execute("SELECT AVG(avg_code_qual) as avg_air_quality FROM air_quality")
        result = cursor.fetchone()
        avg_air_quality = result['avg_air_quality'] if result else 0
        
        # Get green spaces stats
        cursor.execute("SELECT SUM(area_km2) as total_green_area FROM green_spaces WHERE area_km2 > 0")
        result = cursor.fetchone()
        total_green_area = result['total_green_area'] if result else 0
        
        # Get tree density stats
        cursor.execute("SELECT AVG(tree_density) as avg_tree_density FROM tree_density WHERE tree_count > 0")
        result = cursor.fetchone()
        avg_tree_density = result['avg_tree_density'] if result else 0
        
        # Get cooling spaces stats
        cursor.execute("SELECT arrondissement, cooling_space_count FROM cooling_spaces_counts ORDER BY cooling_space_count DESC LIMIT 1")
        result = cursor.fetchone()
        max_cooling_spaces_arr = result['arrondissement'] if result else 'N/A'
        
        # Get average area
        cursor.execute("SELECT AVG(area_km2) as avg_area_km2 FROM green_spaces WHERE area_km2 > 0")
        result = cursor.fetchone()
        avg_area_km2 = result['avg_area_km2'] if result else 0
        
        cursor.close()
        return {
            'avg_air_quality': avg_air_quality or 0,
            'total_green_area': total_green_area or 0,
            'avg_tree_density': avg_tree_density or 0,
            'max_cooling_spaces_arr': max_cooling_spaces_arr,
            'avg_area_km2': avg_area_km2 or 0
        }
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return {
            'avg_air_quality': 0,
            'total_green_area': 0,
            'avg_tree_density': 0,
            'max_cooling_spaces_arr': 'N/A',
            'avg_area_km2': 0
        }
    finally:
        release_db_conn(conn)

@app.get("/api/green-spaces-chart")
async def get_green_spaces_chart():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT arrondissement, area_km2 FROM green_spaces WHERE area_km2 > 0 ORDER BY arrondissement")
        data = pd.DataFrame(cursor.fetchall())
        cursor.close()
        
        if not data.empty:
            create_stunning_chart(data, "enhanced_bar", 
                                 "🌳 Green Spaces Availability by Arrondissement", 
                                 "arrondissement", "area_km2", "green_spaces")
        
        return FileResponse("images/green_spaces.png", media_type="image/png")
    except Exception as e:
        logger.error(f"Error generating green spaces chart: {e}")
        raise
    finally:
        release_db_conn(conn)

@app.get("/api/tree-density-vs-green-spaces")
async def get_tree_density_vs_green_spaces():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT g.arrondissement, g.area_km2, t.tree_density 
            FROM green_spaces g 
            JOIN tree_density t ON g.arrondissement = t.arrondissement 
            WHERE g.area_km2 > 0 AND t.tree_count > 0
        """)
        data = pd.DataFrame(cursor.fetchall())
        cursor.close()
        
        if not data.empty:
            create_stunning_chart(data, "neon_scatter",
                                 "🌲 Tree Density vs Green Spaces Area",
                                 "area_km2", "tree_density", "tree_density_vs_green_spaces")
        
        return FileResponse("images/tree_density_vs_green_spaces.png", media_type="image/png")
    except Exception as e:
        logger.error(f"Error generating tree density vs green spaces chart: {e}")
        raise
    finally:
        release_db_conn(conn)

@app.get("/api/cooling-spaces-chart")
async def get_cooling_spaces_chart():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT arrondissement, cooling_space_count FROM cooling_spaces_counts WHERE cooling_space_count > 0 ORDER BY arrondissement")
        data = pd.DataFrame(cursor.fetchall())
        cursor.close()
        
        if not data.empty:
            create_stunning_chart(data, "holographic_radar",
                                 "❄️ Cooling Spaces Distribution",
                                 "arrondissement", "cooling_space_count", "cooling_spaces")
        
        return FileResponse("images/cooling_spaces.png", media_type="image/png")
    except Exception as e:
        logger.error(f"Error generating cooling spaces chart: {e}")
        raise
    finally:
        release_db_conn(conn)

@app.get("/api/air-quality-trend")
async def get_air_quality_trend():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT year, avg_code_qual FROM air_quality ORDER BY year")
        data = pd.DataFrame(cursor.fetchall())
        cursor.close()
        
        if not data.empty:
            create_stunning_chart(data, "cyberpunk_line",
                                 "🌬️ Air Quality Evolution",
                                 "year", "avg_code_qual", "air_quality_trend")
        
        return FileResponse("images/air_quality_trend.png", media_type="image/png")
    except Exception as e:
        logger.error(f"Error generating air quality trend: {e}")
        raise
    finally:
        release_db_conn(conn)

@app.get("/api/data/air_quality")
async def get_air_quality_data():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT year, avg_code_qual, lib_qual FROM air_quality")
        data = [dict(row) for row in cursor.fetchall()]
        cursor.close()
        return data
    except Exception as e:
        logger.error(f"Error fetching air quality data: {e}")
        return []
    finally:
        release_db_conn(conn)

@app.get("/api/data/green_spaces.csv")
async def get_green_spaces_csv():
    conn = get_db_conn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT arrondissement, area_km2 FROM green_spaces WHERE area_km2 > 0 ORDER BY arrondissement")
        data = pd.DataFrame(cursor.fetchall())
        cursor.close()
        
        output = io.StringIO()
        data.to_csv(output, index=False)
        return Response(content=output.getvalue(), media_type="text/csv", 
                       headers={"Content-Disposition": "attachment; filename=green_spaces.csv"})
    except Exception as e:
        logger.error(f"Error generating green spaces CSV: {e}")
        raise
    finally:
        release_db_conn(conn)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Paris Environment Dashboard is running! 🚀"}

@app.on_event("shutdown")
def shutdown_event():
    db_pool.closeall()
    logger.info("Database connection pool closed")