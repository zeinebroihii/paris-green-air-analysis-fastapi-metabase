# paris-green-air-analysis-fastapi-metabase# Paris Green-Air Analysis (FastAPI + Metabase)

End-to-end open-data pipeline to explore **green spaces, air quality, and urban cooling in Paris**.  
Data flows from public APIs → processing & spatial analysis → PostgreSQL/PostGIS → FastAPI API → interactive Metabase dashboards.

---

##  Project Structure

```
paris-green-air-analysis-fastapi-metabase/
├── data/                  # Raw & processed exports (CSV/JSON, ignored in git)
├── scripts/               # ETL mini-algorithms
│   ├── fetch_data.py      # API fetch + scraping fallback
│   ├── process_data.py    # Cleaning, joins, stats
│   └── load_to_db.py      # Load processed data into PostGIS
├── app/                   # FastAPI backend
│   ├── main.py            # API endpoints
│   ├── models.py          # SQLAlchemy + Pydantic models
│   ├── database.py        # DB session & engine
│   └── requirements.txt   # Python dependencies
├── metabase/              # Optional: exported dashboard configs / sample SQL
│   └── setup.sql
├── static/                # Optional: HTML to embed Metabase dashboards
│   └── dashboard.html
├── Dockerfile             # FastAPI container
├── docker-compose.yml     # Orchestrates DB + FastAPI + Metabase
├── .env.example           # Env vars (DB creds, Metabase admin)
├── .gitignore
├── README.md
└── analysis_report.md     # Insights, screenshots, findings
```

---

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/zeinebroihii/paris-green-air-analysis-fastapi-metabase.git
cd paris-green-air-analysis-fastapi-metabase
cp .env.example .env   # update credentials if needed
```

### 2. Run with Docker
```bash
docker-compose up -d
```
This spins up:
- **PostGIS DB** (`db` service, port 5432)
- **FastAPI API** (`fastapi` service, port 8000)
- **Metabase** (`metabase` service, port 3000)

### 3. Fetch & Process Data
Inside the container or host environment:
```bash
python scripts/fetch_data.py
python scripts/process_data.py
python scripts/load_to_db.py
```
This downloads open data, cleans/joins, and loads it into PostGIS.

### 4. Explore
- FastAPI Swagger UI: **http://localhost:8000/docs**
- Metabase UI: **http://localhost:3000**

---

## 🛡️ Components

| Layer          | Tech & Purpose                                                                                  |
|----------------|--------------------------------------------------------------------------------------------------|
| **ETL**        | `requests`, `BeautifulSoup`, `pandas`, `geopandas` to fetch Paris Open Data & Airparif APIs.      |
| **Database**   | PostgreSQL + PostGIS for spatial joins & analytics.                                              |
| **Backend**    | FastAPI serves JSON endpoints and optional HTML dashboard embeds.                                |
| **Visualization** | Metabase for interactive maps, correlations, and reports.                                     |
| **Deployment** | Docker & docker-compose with health checks and 12-factor env configuration.                      |

---

## Development

Local run without Docker:
```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r app/requirements.txt
uvicorn app.main:app --reload
```

---

## 📊 Example Analysis

- Tree density vs. NO₂ correlation per arrondissement.
- Green space coverage vs. particulate matter (PM2.5) levels.
- Cooling-space accessibility.

(See **analysis_report.md** for findings and Metabase screenshots.)

---


Copy `.env.example` to `.env` and fill in:
```
POSTGRES_USER=user
POSTGRES_PASSWORD=pass
MB_ADMIN_EMAIL=admin@example.com
MB_ADMIN_PASSWORD=123456
DATABASE_URL=postgresql://user:pass@db:5432/paris_data
```

---


- **Branching:** `main` for production, `dev` for experiments.  
- **Commits:** Conventional messages (`feat:`, `fix:`, `docs:`…).  
- **Data:** Large raw files (>10 MB) are `.gitignore`d.  
- **Monitoring:** Docker healthchecks for DB & Metabase.  
- **Reproducibility:** Export Metabase dashboards to `metabase/` for versioning.

---


- [ ] Add automated cron job to refresh datasets.
- [ ] Expand API endpoints (e.g., geo-filtered queries).
- [ ] Deploy to cloud (Render/Heroku/VM) with SSL.

---

**Author:** Zeineb

