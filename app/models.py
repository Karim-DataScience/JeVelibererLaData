from pydantic import BaseModel
from typing import List, Optional
import datetime as dt

# =================================================================
# 2. PYDANTIC MODELS (SCHEMAS GROUPÉS)
# =================================================================

# --- Base/Write Models (pour les requêtes POST/PUT) ---
class StationBase(BaseModel):
    station_code: str
    name: str
    latitude: float
    longitude: float
    type: str

class VeloBase(BaseModel):
    velo_name: str
    bikeelectric: Optional[bool] = None

# --- Read/Dimension Models (pour les réponses GET) ---
class StationRead(StationBase):
    nbdock_total: Optional[int] = None
    maxbikeoverflow: Optional[int] = None

    class Config:
        from_attributes = True # Importante pour mapper les résultats SQL

class VeloRead(VeloBase):
    class Config:
        from_attributes = True
        
# --- Fact/Transaction Models ---
class EtatStation(BaseModel):
    station_code: str
    timestamp_capture: dt.datetime
    nbbike: int
    nbebike: int
    nbfreedock: int
    state: str

    class Config:
        from_attributes = True

class LocalisationVeloRead(BaseModel):
    loc_id: int
    snapshot_id: int
    velo_name: str
    station_code: str
    bikestatus: str
    
    class Config:
        from_attributes = True

# --- Analysis Models ---
class Trajet(BaseModel):
    velo_name: str
    station_depart_code: str
    heure_depart: dt.datetime
    station_arrivee_code: str
    heure_arrivee: dt.datetime
    duree_trajet_minutes: float

    class Config:
        from_attributes = True

class TrajetStats(BaseModel):
    station_depart_code: str
    station_arrivee_code: str
    nombre_trajets: int
    duree_moyenne_minutes: float

    class Config:
        from_attributes = True

class TrajetsByDayStats(BaseModel):
    jour: dt.date
    nombre_trajets: int
    duree_moyenne_minutes: float

    class Config:
        from_attributes = True

class StationTraffic(BaseModel):
    station_code: str
    station_name: str
    type_flux: str
    nombre_flux: int

    class Config:
        from_attributes = True

class StationFlowImbalance(BaseModel):
    station_code: str
    station_name: str
    departures: int
    arrivals: int
    imbalance: int

    class Config:
        from_attributes = True

class TopVelo(BaseModel):
    velo_name: str
    nombre_trajets: int
    duree_totale_heures: float

    class Config:
        from_attributes = True

class AverageRouteStats(BaseModel):
    station_depart_code: str
    station_arrivee_code: str
    duree_moyenne_minutes: float

    class Config:
        from_attributes = True


class VeloStationsCount(BaseModel):
    velo_name: str
    periode: str
    nombre_stations_visitees: int

    class Config:
        from_attributes = True
