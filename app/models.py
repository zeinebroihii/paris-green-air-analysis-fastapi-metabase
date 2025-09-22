from sqlalchemy import Column, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from database import Base

# SQLAlchemy model for the analysis_results table
class AnalysisResult(Base):
    __tablename__ = "analysis_results"
    id = Column(Integer, primary_key=True, index=True)  # Auto-incrementing ID, indexed for queries
    c_ar = Column(Integer, nullable=False)  # Arrondissement code (e.g., 1 for 1er)
    tree_count = Column(Integer, nullable=True)  # Number of trees in arrondissement
    density_per_km2 = Column(Float, nullable=True)  # Trees per km²
    green_area_m2 = Column(Float, nullable=True)  # Total green space area (m²)
    coverage_per_km2 = Column(Float, nullable=True)  # Green area per km²
    cooling_count = Column(Integer, nullable=True)  # Number of cooling spaces
    no2 = Column(Float, nullable=True)  # Average NO2 concentration (µg/m³)
    pm25 = Column(Float, nullable=True)  # Average PM2.5 concentration (µg/m³)

# Pydantic model for API validation and serialization
class AnalysisSchema(BaseModel):
    c_ar: int
    tree_count: int | None  # Optional to handle missing data
    density_per_km2: float | None
    green_area_m2: float | None
    coverage_per_km2: float | None
    cooling_count: int | None
    no2: float | None
    pm25: float | None

    class Config:
        orm_mode = True  # Enables direct conversion from SQLAlchemy objects