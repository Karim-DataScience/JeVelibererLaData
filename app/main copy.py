from fastapi import FastAPI, Depends, HTTPException, APIRouter, Query, status
# Import correct de l'AsyncSession pour le typage
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
import datetime as dt

# Import your models
from app.models import StationBase, VeloBase, StationRead, VeloRead, EtatStation, LocalisationVeloRead, Trajet, TrajetStats, TrajetsByDayStats, StationTraffic, StationFlowImbalance, TopVelo, AverageRouteStats
# Importez l'AsyncSession, get_db et l'app depuis database.py
from app.database import get_db, app


# DÉFINITION UNIQUE ET PROPRE DES ROUTERS
router_dims = APIRouter(prefix="/api/v1/dimensions", tags=["Dimensions (CRUD)"])
router_facts = APIRouter(prefix="/api/v1/facts", tags=["Faits (Lecture/Analyse)"])
router_analysis = APIRouter(prefix="/api/v1/analysis", tags=["Analyse (V_TRAJETS)"])


# =================================================================
# 3. ROUTER DIMENSIONS (CRUD COMPLET)
# * Utilise AsyncSession et des fonctions async partout.
# =================================================================

# ----- STATION CRUD -----

@router_dims.get("/stations", response_model=List[StationRead], summary="Liste de toutes les stations")
async def read_all_stations(db: AsyncSession = Depends(get_db)):
    # Utilisation de await pour l'exécution asynchrone
    result = await db.execute(text("SELECT * FROM station ORDER BY station_code"))
    # Utilisation de .all() sur le résultat asynchrone
    rows = result.all()
    return [StationRead.model_validate(row, from_attributes=True) for row in rows]

@router_dims.get("/stations/{code}", response_model=StationRead, summary="Détails d'une station par code")
async def read_station(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT * FROM station WHERE station_code = :code"), {"code": code})
    row = result.first()
    if row is None:
        raise HTTPException(status_code=404, detail="Station not found")
    return StationRead.model_validate(row, from_attributes=True)

@router_dims.post("/stations", status_code=status.HTTP_201_CREATED, summary="Créer/Mettre à jour une station (UPSERT)")
async def create_station(station: StationBase, db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(
            text("""
                INSERT INTO station (station_code, name, latitude, longitude, type)
                VALUES (:station_code, :name, :latitude, :longitude, :type)
                ON CONFLICT (station_code) DO UPDATE 
                SET name = EXCLUDED.name, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, type = EXCLUDED.type
                RETURNING station_code;
            """),
            station.model_dump()
        )
        await db.commit() # Commit asynchrone
        return {"message": f"Station {station.station_code} created or updated"}
    except Exception as e:
        await db.rollback() # Rollback asynchrone
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@router_dims.delete("/stations/{code}", status_code=status.HTTP_204_NO_CONTENT, summary="Supprimer une station")
async def delete_station(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("DELETE FROM station WHERE station_code = :code RETURNING station_code;"), {"code": code})
    
    # rowcount est une propriété synchrone même avec AsyncSession.
    # Pour s'assurer du nombre de lignes affectées, on peut aussi utiliser .scalar_one_or_none() après l'execute 
    # ou vérifier que la ligne retournée existe si on utilise RETURNING.
    # Ici, nous allons simplement utiliser rowcount qui fonctionne après un execute.
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Station not found")
    
    await db.commit()
    return

# ----- VELO CRUD -----

@router_dims.get("/velos", response_model=List[VeloRead], summary="Liste de tous les vélos")
async def read_all_velos(db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT * FROM velo ORDER BY velo_name"))
    rows = result.all()
    return [VeloRead.model_validate(row, from_attributes=True) for row in rows]

@router_dims.post("/velos", status_code=status.HTTP_201_CREATED, summary="Créer un vélo (ON CONFLICT DO NOTHING)")
async def create_velo(velo: VeloBase, db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(
            text("""
                INSERT INTO velo (velo_name, bikeelectric)
                VALUES (:velo_name, :bikeelectric)
                ON CONFLICT (velo_name) DO NOTHING;
            """),
            velo.model_dump()
        )
        await db.commit()
        return {"message": f"Velo {velo.velo_name} created or ignored"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# =================================================================
# 4. ROUTER FAITS (Lecture d'état et Suppression Ciblée)
# =================================================================

@router_facts.get("/stations/{station_code}/etat_actuel", response_model=EtatStation, summary="État le plus récent d'une station")
async def read_station_current_state(station_code: str, db: AsyncSession = Depends(get_db)):
    """ Récupère le dernier état connu d'une station, basé sur le snapshot le plus récent."""
    sql_query = text("""
        SELECT 
            es.station_code, 
            s.timestamp_capture, 
            es.nbbike, es.nbebike, es.nbfreedock, es.state
        FROM etat_station es
        JOIN snapshot s ON es.snapshot_id = s.snapshot_id
        WHERE es.station_code = :code
        ORDER BY s.timestamp_capture DESC
        LIMIT 1;
    """)
    result = await db.execute(sql_query, {"code": station_code})
    row = result.first()
    
    if row is None:
        raise HTTPException(status_code=404, detail="Station ou état non trouvé")
    
    return EtatStation.model_validate(row, from_attributes=True)

@router_facts.get("/localisations", response_model=List[LocalisationVeloRead], summary="Localisations récentes (Paginée)")
async def read_recent_locations(db: AsyncSession = Depends(get_db), limit: int = 100, offset: int = 0):
    sql_query = text(f"""
        SELECT loc_id, snapshot_id, velo_name, station_code, bikestatus
        FROM localisation_velo
        ORDER BY loc_id DESC
        LIMIT :limit OFFSET :offset;
    """)
    result = await db.execute(sql_query, {"limit": limit, "offset": offset})
    rows = result.all()
    return [LocalisationVeloRead.model_validate(row, from_attributes=True) for row in rows]

@router_facts.delete("/localisations/{loc_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Supprime une ligne de localisation par ID")
async def delete_location(loc_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("DELETE FROM localisation_velo WHERE loc_id = :id RETURNING loc_id;"), {"id": loc_id})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Location ID not found")
    await db.commit()
    return

# =================================================================
# 5. ROUTER ANALYSE (Trajets et Statistiques)
# =================================================================

@router_analysis.get("/trajets", response_model=List[Trajet], summary="Liste des trajets inférés (PAGINÉE)")
async def read_trajets(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    sql_query = text(f"""
        SELECT velo_name, station_depart_code, heure_depart, station_arrivee_code, heure_arrivee, duree_trajet_minutes
        FROM V_TRAJETS
        ORDER BY heure_depart DESC
        LIMIT :limit OFFSET :offset;
    """)
    
    result = await db.execute(sql_query, {"limit": limit, "offset": offset})
    rows = result.all()
    return [Trajet.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/trajets/top_routes", response_model=List[TrajetStats], summary="Top 10 des trajets les plus populaires")
async def get_top_routes(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
):
    sql_query = text(f"""
        SELECT 
            station_depart_code,
            station_arrivee_code,
            COUNT(*) AS nombre_trajets,
            AVG(duree_trajet_minutes) AS duree_moyenne_minutes
        FROM V_TRAJETS
        GROUP BY 1, 2
        ORDER BY nombre_trajets DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, {"limit": limit})
    rows = result.all()
    return [TrajetStats.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/velos/{velo_name}/boomerang", summary="Vérifie si un vélo a effectué un trajet boomerang")
async def check_boomerang(velo_name: str, db: AsyncSession = Depends(get_db)):
    sql_query = text("""
        SELECT COUNT(*)
        FROM V_TRAJETS
        WHERE velo_name = :vn
        AND station_depart_code = station_arrivee_code;
    """)
    
    # Utilisation de .scalar_one() asynchrone pour obtenir la valeur unique
    count = await db.execute(sql_query, {"vn": velo_name})
    count_value = count.scalar_one()
    
    return {"velo_name": velo_name, "is_boomerang_user": count_value > 0, "count": count_value}


@router_analysis.get("/trajets/by_day", response_model=List[TrajetsByDayStats], summary="Statistiques de trajets agrégées par jour")
async def get_trajets_by_day(
    db: AsyncSession = Depends(get_db),
    start_date: Optional[dt.date] = Query(None, description="Date de début (YYYY-MM-DD)"),
    end_date: Optional[dt.date] = Query(None, description="Date de fin (YYYY-MM-DD)")
):
    where_clause = []
    params = {}
    
    if start_date:
        where_clause.append("date(heure_depart) >= :start_date")
        params['start_date'] = start_date
        
    if end_date:
        where_clause.append("date(heure_depart) <= :end_date")
        params['end_date'] = end_date
        
    where_sql = f"WHERE {' AND '.join(where_clause)}" if where_clause else ""

    sql_query = text(f"""
        SELECT 
            date(heure_depart) AS jour, 
            COUNT(*) AS nombre_trajets,
            AVG(duree_trajet_minutes) AS duree_moyenne_minutes
        FROM V_TRAJETS
        {where_sql}
        GROUP BY 1
        ORDER BY jour;
    """)
    
    result = await db.execute(sql_query, params)
    rows = result.all()
    return [TrajetsByDayStats.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/stations/top_source_destination", response_model=List[StationTraffic], summary="Top N des stations générant le plus de départs/arrivées")
async def get_top_source_destination(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100)
):
    sql_query = text(f"""
        WITH Flux AS (
            SELECT station_depart_code AS station_code, 'Depart' AS type_flux, COUNT(*) AS nombre_flux FROM V_TRAJETS GROUP BY 1
            UNION ALL
            SELECT station_arrivee_code AS station_code, 'Arrivee' AS type_flux, COUNT(*) AS nombre_flux FROM V_TRAJETS GROUP BY 1
        )
        SELECT 
            f.station_code,
            s.name AS station_name,
            f.type_flux,
            SUM(f.nombre_flux) AS nombre_flux
        FROM Flux f
        JOIN station s ON s.station_code = f.station_code
        GROUP BY f.station_code, s.name, f.type_flux
        ORDER BY nombre_flux DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, {"limit": limit})
    rows = result.all()
    return [StationTraffic.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/stations/flow_imbalance", response_model=List[StationFlowImbalance], summary="Déséquilibre de flux (Départs - Arrivées)")
async def get_flow_imbalance(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100)
):
    sql_query = text(f"""
        WITH Departures AS (
            SELECT station_depart_code AS code, COUNT(*) AS departures FROM V_TRAJETS GROUP BY 1
        ),
        Arrivals AS (
            SELECT station_arrivee_code AS code, COUNT(*) AS arrivals FROM V_TRAJETS GROUP BY 1
        )
        SELECT 
            s.station_code,
            s.name AS station_name,
            COALESCE(d.departures, 0) AS departures,
            COALESCE(a.arrivals, 0) AS arrivals,
            (COALESCE(d.departures, 0) - COALESCE(a.arrivals, 0)) AS imbalance
        FROM station s
        LEFT JOIN Departures d ON s.station_code = d.code
        LEFT JOIN Arrivals a ON s.station_code = a.code
        ORDER BY ABS(imbalance) DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, {"limit": limit})
    rows = result.all()
    return [StationFlowImbalance.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/velos/top_used", response_model=List[TopVelo], summary="Top N des vélos les plus utilisés (par nombre de trajets)")
async def get_top_used_velos(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=50)
):
    sql_query = text(f"""
        SELECT 
            velo_name,
            COUNT(*) AS nombre_trajets,
            SUM(duree_trajet_minutes) / 60.0 AS duree_totale_heures
        FROM V_TRAJETS
        GROUP BY velo_name
        ORDER BY nombre_trajets DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, {"limit": limit})
    rows = result.all()
    return [TopVelo.model_validate(row, from_attributes=True) for row in rows]


@router_analysis.get("/trajets/average_by_route", response_model=List[AverageRouteStats], summary="Durée moyenne des trajets par paire de stations")
async def get_average_duration_by_route(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=50)
):
    sql_query = text(f"""
        SELECT 
            station_depart_code,
            station_arrivee_code,
            AVG(duree_trajet_minutes) AS duree_moyenne_minutes
        FROM V_TRAJETS
        GROUP BY 1, 2
        ORDER BY duree_moyenne_minutes DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, {"limit": limit})
    rows = result.all()
    return [AverageRouteStats.model_validate(row, from_attributes=True) for row in rows]


# =================================================================
# 6. ENREGISTREMENT DES ROUTERS (Final)
# =================================================================

app.include_router(router_dims)
app.include_router(router_facts)
app.include_router(router_analysis)

# Route racine pour l'état de l'API
@app.get("/", tags=["Status"])
def root():
    return {"message": "Vélib Analysis API operational. Check /docs for endpoints."}