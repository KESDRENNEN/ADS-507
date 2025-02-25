from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import aliased
from sqlalchemy import func

# Database connection (PostgreSQL RDS)
engine = create_engine("postgresql://MattAdmin:MammaJamma015!!@ads507db.cbyiqqq0cd2d.us-east-2.rds.amazonaws.com:5432/ads507db")

Base = declarative_base()

# Session setup
Session = sessionmaker(bind=engine)
session = Session()

# Read the cleaned data (assuming the files are available locally)
fema_df = pd.read_csv("/Users/mammajamma/Desktop/507Project/ADS-507_NEW/scripts/prod/fema_data.csv")
#climate_df = pd.read_csv("cleaned_climate_data.csv")
bls_df = pd.read_csv("/Users/mammajamma/Desktop/507Project/ADS-507_NEW/scripts/prod/bls_data.csv")

# Define Tables using SQLAlchemy ORM

class Disaster(Base):
    __tablename__ = "disasters"
    disasterNumber = Column(Integer, primary_key=True)
    femaDeclarationString = Column(String)
    state = Column(String(2))
    declarationType = Column(String(10))
    declarationDate = Column(Date)
    incidentType = Column(String)
    incidentBeginDate = Column(Date)
    incidentEndDate = Column(Date)
    region = Column(Integer)

class Climate(Base):
    __tablename__ = "climate"
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    avg_temp = Column(Float)
    co2_emissions = Column(Float)
    sea_level_rise = Column(Float)
    rainfall = Column(Float)
    population = Column(Float)
    renewable_energy = Column(Float)
    extreme_weather_events = Column(Integer)
    forest_area = Column(Float)

class BLS(Base):
    __tablename__ = "bls"
    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String)
    year = Column(Integer)
    period = Column(String)
    value = Column(Float)

# Create all tables in RDS (if they don't exist)
Base.metadata.create_all(engine)
print("✅ Tables created successfully!")

# Insert data into tables using session.add_all
disasters = [
    Disaster(
        disasterNumber=row["disasterNumber"],
        femaDeclarationString=row.get("femaDeclarationString", None),
        state=row.get("state", None),
        declarationType=row.get("declarationType", None),
        declarationDate=row.get("declarationDate", None),
        incidentType=row.get("incidentType", None),
        incidentBeginDate=row.get("incidentBeginDate", None),
        incidentEndDate=row.get("incidentEndDate", None),
        region=row.get("region", None)
    )
    for _, row in fema_df.iterrows()
]

session.add_all(disasters)
session.commit()
print("✅ FEMA data inserted successfully!")

# Insert climate data
climate_records = [
    Climate(
        year=int(row["year"]),
        avg_temp=row["avg_temp"],
        co2_emissions=row["co2_emissions"],
        sea_level_rise=row["sea_level_rise"],
        rainfall=row["rainfall"],
        population=row["population"],
        renewable_energy=row["renewable_energy"],
        extreme_weather_events=int(row["extreme_weather_events"]),
        forest_area=row["forest_area"]
    )
    for _, row in climate_df.iterrows()
]

session.add_all(climate_records)
session.commit()
print("✅ Climate data inserted successfully!")

# Insert BLS data
bls_records = [
    BLS(
        series_id=row["series_id"],
        year=int(row["year"]),
        period=row["period"],
        value=row["value"]
    )
    for _, row in bls_df.iterrows()
]

session.add_all(bls_records)
session.commit()
print("✅ BLS data inserted successfully!")

# Confirm rows in tables
for table in ["disasters", "climate", "bls"]:
    count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    print(f"📊 {table} table has {count} rows")
