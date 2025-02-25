
!pip install sqlalchemy pandas
!pip install matplotlib
!pip install seaborn


from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from sqlalchemy import text
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import aliased
from sqlalchemy import func
import matplotlib.pyplot as plt
import seaborn as sns

engine = create_engine("postgresql://MattAdmin:MammaJamma015!!@ads507db.cbyiqqq0cd2d.us-east-2.rds.amazonaws.com:5432/ads507db")


Base = declarative_base()


Session = sessionmaker(bind=engine)
session = Session()


fema_df = pd.read_csv("cleaned_fema_data.csv")
climate_df = pd.read_csv("cleaned_climate_data.csv")
bls_df = pd.read_csv("cleaned_bls_data.csv")


#FEMA Table

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


#Climate Table

class Climate(Base):
    __tablename__ = "climate"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID
    year = Column(Integer, nullable=False)  # Allow duplicates
    avg_temp = Column(Float)
    co2_emissions = Column(Float)
    sea_level_rise = Column(Float)
    rainfall = Column(Float)
    population = Column(Float)
    renewable_energy = Column(Float)
    extreme_weather_events = Column(Integer)
    forest_area = Column(Float)


#BLS Tables

class BLS(Base):
    __tablename__ = "bls"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String)
    year = Column(Integer)
    period = Column(String)
    value = Column(Float)


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
print("✅ Tables created successfully!")


#adjust FEMA data

fema_df["declarationDate"] = pd.to_datetime(fema_df["declarationDate"])
fema_df["incidentBeginDate"] = pd.to_datetime(fema_df["incidentBeginDate"])
fema_df["incidentEndDate"] = pd.to_datetime(fema_df["incidentEndDate"])

fema_df = fema_df.drop_duplicates(subset=["disasterNumber"])


#adjust climate data

climate_df.rename(columns={
    "Year": "year",
    "Avg Temperature (°C)": "avg_temp",
    "CO2 Emissions (Tons/Capita)": "co2_emissions",
    "Sea Level Rise (mm)": "sea_level_rise",
    "Rainfall (mm)": "rainfall",
    "Population": "population",
    "Renewable Energy (%)": "renewable_energy",
    "Extreme Weather Events": "extreme_weather_events",
    "Forest Area (%)": "forest_area"
}, inplace=True)

# Ensure numeric data is in the correct format
numeric_columns = ["year", "avg_temp", "co2_emissions", "sea_level_rise",
                   "rainfall", "population", "renewable_energy",
                   "extreme_weather_events", "forest_area"]

climate_df[numeric_columns] = climate_df[numeric_columns].astype(float)


#adjust bls

bls_df["year"] = bls_df["year"].astype(int)
bls_df["value"] = bls_df["value"].astype(float)


#insert FEMA data

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


#insert climate data

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

#insert BLS data

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

#confirming rows in tables

for table in ["disasters", "climate", "bls"]:
    count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    print(f"📊 {table} table has {count} rows")

# %%
#creating table Aliases

climate_alias = aliased(Climate)
bls_alias = aliased(BLS)


#joining climate disasters with BLS by year

query = session.query(
    climate_alias.year.label("year"),
    func.avg(climate_alias.avg_temp).label("avg_temp"),
    func.avg(climate_alias.co2_emissions).label("co2_emissions"),
    func.avg(climate_alias.sea_level_rise).label("sea_level_rise"),
    func.avg(climate_alias.rainfall).label("avg_rainfall"),
    func.avg(climate_alias.renewable_energy).label("renewable_energy"),
    func.avg(climate_alias.extreme_weather_events).label("extreme_weather_events"),
    func.avg(bls_alias.value).label("avg_unemployment_rate")  # Aggregate unemployment rate
).join(
    bls_alias, climate_alias.year == bls_alias.year
).group_by(
    climate_alias.year
).order_by(
    climate_alias.year
)


climate_bls_df = pd.DataFrame(query.all(), columns=[
    "year", "avg_temp", "co2_emissions", "sea_level_rise",
    "avg_rainfall", "renewable_energy", "extreme_weather_events",
    "avg_unemployment_rate"])





