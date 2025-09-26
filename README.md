# Paris Green-Air Analysis (FastAPI + DevOps)

Automated open-data pipeline to explore **green spaces, air quality, and urban cooling in Paris**.  
Data flows from public APIs → automated processing & spatial analysis → PostgreSQL/PostGIS → FastAPI API → interactive visualizations.  
This project provides a 2025 baseline for the post-2024 Olympics green legacy, offering insights for sustainable urban planning.

---

## Project Structure
paris-green-air-analysis-fastapi-metabase/
├── app/                   # FastAPI backend
│   ├── main.py            # API endpoints with 4 charts and statistics
│   ├── database.py        # DB session & engine
│   └── requirements.txt   # Python dependencies
├── data/                  # Raw & processed exports (CSV, ignored in git)
│   ├── processed/         # Processed data (joined tables with CSV form)
│   └── raw/               # Raw data files (CSV from APIs)
├── images/                # Auto-generated chart images
├── scripts/               # ETL mini-algorithms
│   ├── fetch_data.py      # API fetch + scraping fallback
│   ├── process_data.py    # Cleaning, joins, stats
│   └── load_to_db.py      # Load processed data into PostGIS
├── static/                # Static files
│   └── index.html         # Dashboard with chart objectives
├── venv/                  # Virtual environment (ignored in git)
├── .env                   # Environment variables (copy from .env.example)
├── .env.example           # Example env vars template
├── .gitignore             # Ignores large files, env, venv
├── docker-compose.yml     # Orchestrates DB + FastAPI
├── Dockerfile             # FastAPI container
├── Dockerfile.app         # Application container
├── .github/workflows/     # GitHub Actions CI/CD pipeline
│   └── ci-cd.yml          # Automates testing and Docker builds
├── README.md              # This file

## Quick Start : 
## note : the project can be cloned and run it locally and can be acccessed through heroku deployment website 

### 1. Clone & Setup (lcoal running )
```bash
git clone https://github.com/zeinebroihii/paris-green-air-analysis-fastapi-metabase.git
cd paris-green-air-analysis-fastapi-metabase
cp .env.example .env   # update credentials if needed 
2. Run with Docker-compose locally containers will be launched in your Docker-desktop
bash docker-compose up -d --build
This spins up:

PostGIS DB (db service, port 5432)
FastAPI API (fastapi service, port 8000)
Application itself that will load instantly the processed dat into your DB

3. Explore

FastAPI Dashboard: http://localhost:8000
API Docs: http://localhost:8000/docs

Note: Data processing and loading are automated via GitHub Actions on dev and main pushes.

🛡️ Components

LayerTech & PurposeETLrequests, pandas, geopandas to fetch Paris Open Data & process spatially (automated).DatabasePostgreSQL + PostGIS for spatial joins & analytics.
BackendFastAPI serves JSON endpoints and HTML dashboard with 4 charts.
VisualizationMatplotlib charts embedded in FastAPI.
DevOpsGitHub Actions for CI/CD (linting, Docker builds,and data automation).

Languages & Frameworks

Python: Core language for ETL scripts, backend, and data processing.
Pandas: Data manipulation and analysis.
Geopandas: Spatial data handling and joins.
Requests: API data fetching.
Matplotlib: Visualization of charts.

FastAPI: Modern, high-performance web framework for the API and dashboard.
PostgreSQL/PostGIS: Relational database with spatial extensions for geospatial analysis.
Docker: Containerization for consistent deployment.
GitHub Actions: CI/CD automation for testing, building, and deployment.


Development
Branching Strategy

dev: For ongoing development and experiments.
main: For production-ready code.
Merging: After testing on dev, merge to main:

Push changes to dev: git push origin dev
Create a Pull Request (PR) in GitHub: Go to "Pull requests" → "New pull request" → Compare dev with main.
Review and merge: Ensure GitHub Actions CI/CD passes, then merge dev into main.


📊 Chart Objectives

Green Spaces Availability: Identify arrondissements with the most/least green spaces to highlight potential overpopulation or verdure gaps.
Tree Density vs Green Spaces: Understand if larger green spaces correlate with higher tree density, identifying areas needing tree planting.
Tree Density vs Air Quality: Test if higher tree density improves air quality, supporting post-2024 sustainability insights.
Cooling Spaces by Arrondissement: Identify well-equipped arrondissements for cooling during heatwaves, detecting vulnerable areas.
Cooling Spaces vs Tree Density: Assess if tree density enhances cooling space effectiveness, aiding urban cooling strategies.
Green Spaces vs Vegetation Proxy: Verify if more green spaces correlate with higher vegetation coverage for planning purposes.


DevOps with GitHub Actions ( initiation for devops automation deployment displays in later stages.. )
CI/CD Pipeline

Workflow: .github/workflows/ci-cd.yml automates linting, Docker builds, vulnerability scans, and data processing.
Triggers: On push to dev/main or pull requests.
Steps:

Checkout code.
Set up Python 3.9 and install dependencies.
Run automated ETL scripts (fetch_data.py, process_data.py, load_to_db.py).
Build Docker images.
Push Docker images


Setup

Secrets:

Go to repo → Settings → Secrets and variables → Actions.
Add:

DOCKERHUB_USERNAME: Your Docker Hub username.
DOCKERHUB_TOKEN: Docker Hub access token (create at Docker Hub → Security → New Access Token).


Update ci-cd.yml with your IMAGE_NAME (e.g., zeinebroihii/fastapi-environment-app).
Commit and push: git add .github/workflows/ci-cd.yml && git commit -m "Add CI/CD pipeline" && git push origin dev.



Author
Zeineb