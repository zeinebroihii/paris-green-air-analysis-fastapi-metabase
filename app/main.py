from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
import json
from database import SessionLocal, engine
from models import AnalysisResult  # Define Pydantic/SQL models in models.py

app = FastAPI(title="Paris Green Analysis API")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/analysis")
def get_analysis(db: Session = Depends(get_db)):
    results = db.query(AnalysisResult).all()  # Assume model
    return {"data": [r.__dict__ for r in results]}

@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("../static/dashboard.html") as f:
        return f.read()  # Serve simple HTML linking to Metabase

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)