from fastapi import FastAPI, Depends, HTTPException, APIRouter, Query, status, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
import datetime as dt
import os
from dotenv import load_dotenv

# Importez vos modèles Pydantic et la configuration DB/App
# Assurez-vous que ces chemins d'import sont corrects pour votre structure de dossier
from models import (
    StationBase, VeloBase, StationRead, VeloRead, EtatStation, 
    LocalisationVeloRead, Trajet, TrajetStats, TrajetsByDayStats, 
    StationTraffic, StationFlowImbalance, TopVelo, AverageRouteStats,VeloStationsCount
)
from database import get_db, app # get_db fournit l'AsyncSession

# --- Configuration de Sécurité ---
load_dotenv()
API_KEY_SECRET = os.getenv("API_KEY_SECRET")

# Dépendance pour l'authentification par Clé API
def api_key_auth(api_key: str = Header(..., alias="X-API-Key")):
    """
    Vérifie la clé API fournie dans le header 'X-API-Key'.
    """
    if API_KEY_SECRET is None or api_key != API_KEY_SECRET:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clé API Invalide. Fournissez un header X-API-Key valide."
        )
    return api_key


# DÉFINITION UNIQUE ET PROPRE DES ROUTERS
router_dims = APIRouter(prefix="/api/v1/dimensions", tags=["Dimensions (CRUD)"])
router_facts = APIRouter(prefix="/api/v1/facts", tags=["Faits (Lecture/Analyse)"])
router_analysis = APIRouter(prefix="/api/v1/analysis", tags=["Analyse (V_TRAJETS)"])


# =================================================================
# 3. ROUTER DIMENSIONS (CRUD COMPLET)
# (CRUD asynchrone, Sécurité POST/DELETE, Filtres)
# =================================================================

# ----- STATION CRUD & LECTURE AVEC FILTRE -----

@router_dims.get("/stations", response_model=List[StationRead], summary="Liste de toutes les stations (avec filtre optionnel par type)")
async def read_all_stations(
    db: AsyncSession = Depends(get_db),
    station_type: Optional[str] = Query(None, description="Filtrer par type de station (ex: 'STANDARD' ou 'PLUS')")
):
    sql_query = "SELECT * FROM station"
    params = {}
    
    if station_type:
        sql_query += " WHERE type = :st_type"
        params["st_type"] = station_type
        
    sql_query += " ORDER BY station_code"
        
    result = await db.execute(text(sql_query), params)
    rows = result.all()
    return [StationRead.model_validate(row, from_attributes=True) for row in rows]


@router_dims.get("/stations/{code}", response_model=StationRead, summary="Détails d'une station par code")
async def read_station(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT * FROM station WHERE station_code = :code"), {"code": code})
    row = result.first()
    if row is None:
        raise HTTPException(status_code=404, detail="Station not found")
    return StationRead.model_validate(row, from_attributes=True)


@router_dims.post("/stations", 
                  status_code=status.HTTP_201_CREATED, 
                  summary="Créer/Mettre à jour une station (UPSERT)",
                  dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
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
        await db.commit()
        return {"message": f"Station {station.station_code} created or updated"}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router_dims.delete("/stations/{code}", 
                    status_code=status.HTTP_204_NO_CONTENT, 
                    summary="Supprimer une station",
                    dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def delete_station(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("DELETE FROM station WHERE station_code = :code RETURNING station_code;"), {"code": code})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Station not found")
    await db.commit()
    return

# ----- VELO CRUD & LECTURE AVEC FILTRE -----

@router_dims.get("/velos", response_model=List[VeloRead], summary="Liste de tous les vélos (avec filtres)")
async def read_all_velos(
    db: AsyncSession = Depends(get_db),
    is_electric: Optional[bool] = Query(None, alias="electric", description="Filtrer par vélo électrique"),
    search: Optional[str] = Query(None, description="Rechercher par nom (partiel)")
):
    conditions = []
    params = {}
    
    if is_electric is not None:
        conditions.append("bikeelectric = :is_electric")
        params['is_electric'] = is_electric
        
    if search:
        conditions.append("velo_name ILIKE :search_term")
        params['search_term'] = f"%{search}%"
        
    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql_query = text(f"SELECT * FROM velo {where_clause} ORDER BY velo_name")
    
    result = await db.execute(sql_query, params)
    rows = result.all()
    return [VeloRead.model_validate(row, from_attributes=True) for row in rows]


@router_dims.post("/velos", 
                  status_code=status.HTTP_201_CREATED, 
                  summary="Créer un vélo (ON CONFLICT DO NOTHING)",
                  dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
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
    


@router_dims.put("/stations/{code}", 
                 response_model=StationRead, 
                 summary="Mettre à jour une station existante par code",
                 dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def update_station(
    code: str, 
    station: StationBase, # Utilise le modèle de base pour les données à mettre à jour
    db: AsyncSession = Depends(get_db)
):
    try:
        # Vérification si la station existe
        check_result = await db.execute(text("SELECT station_code FROM station WHERE station_code = :code"), {"code": code})
        if check_result.first() is None:
            raise HTTPException(status_code=404, detail=f"Station code {code} not found")

        # Mise à jour des données
        await db.execute(
            text("""
                UPDATE station
                SET name = :name, latitude = :latitude, longitude = :longitude, type = :type
                WHERE station_code = :code;
            """),
            {
                "code": code,
                "name": station.name,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "type": station.type
            }
        )
        await db.commit()
        
        # Récupération de l'objet mis à jour pour le renvoyer
        result = await db.execute(text("SELECT * FROM station WHERE station_code = :code"), {"code": code})
        updated_row = result.first()
        
        return StationRead.model_validate(updated_row, from_attributes=True)
    
    except HTTPException:
        # Relance l'exception 404 si elle a été levée
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error during update: {e}")
    



@router_dims.put("/velos/{velo_name}", 
                 response_model=VeloRead, 
                 summary="Mettre à jour un vélo existant par nom",
                 dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def update_velo(
    velo_name: str, 
    velo: VeloBase,
    db: AsyncSession = Depends(get_db)
):
    try:
        # La mise à jour est simple car la seule colonne modifiable est bikeelectric
        result = await db.execute(
            text("""
                UPDATE velo
                SET bikeelectric = :bikeelectric
                WHERE velo_name = :vn
                RETURNING *; -- Retourne la ligne mise à jour
            """),
            {
                "vn": velo_name,
                "bikeelectric": velo.bikeelectric
            }
        )
        updated_row = result.first()

        if updated_row is None:
            raise HTTPException(status_code=404, detail=f"Velo {velo_name} not found")

        await db.commit()
        
        return VeloRead.model_validate(updated_row, from_attributes=True)
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error during update: {e}")

@router_dims.delete("/stations/{code}", 
                    status_code=status.HTTP_204_NO_CONTENT, 
                    summary="Supprimer une station (confirmation requise)",
                    dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def delete_station(
    code: str, 
    confirm: bool = Query(False, description="Doit être mis à 'true' pour confirmer la suppression."), # <-- Ajout du Query Parameter
    db: AsyncSession = Depends(get_db)
):
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation requise. Ajoutez le paramètre de requête ?confirm=true pour procéder à la suppression."
        )

    # Si la confirmation est donnée, on procède à la suppression
    result = await db.execute(text("DELETE FROM station WHERE station_code = :code RETURNING station_code;"), {"code": code})
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Station not found")
        
    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error during deletion: {e}")
        
    return # Réponse 204 No Content

# Dans main.py, sous router_dims

@router_dims.delete("/velos/{velo_name}", 
                    status_code=status.HTTP_204_NO_CONTENT, 
                    summary="Supprimer un vélo (confirmation requise)",
                    dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def delete_velo(
    velo_name: str, 
    confirm: bool = Query(False, description="Doit être mis à 'true' pour confirmer la suppression."),
    db: AsyncSession = Depends(get_db)
):
    """
    Supprime un vélo de la table de dimension `velo`. 
    Nécessite le header X-API-Key et le Query Parameter `confirm=true`.
    """
    
    # 1. Vérification de la Confirmation
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation requise. Ajoutez le paramètre de requête ?confirm=true pour procéder à la suppression du vélo."
        )

    try:
        # 2. Exécution de la Suppression
        # NOTE : Si d'autres tables (faits/trajets) ont des clés étrangères vers velo_name, 
        # cette opération échouera à moins que la DB ne gère la suppression en cascade (ON DELETE CASCADE).
        result = await db.execute(
            text("DELETE FROM velo WHERE velo_name = :vn RETURNING velo_name;"), 
            {"vn": velo_name}
        )
        
        # 3. Vérification du Résultat
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Velo {velo_name} not found")
            
        # 4. Validation de la Transaction
        await db.commit()
        
    except HTTPException:
        # Permet aux exceptions 404/400 de passer
        raise
    except Exception as e:
        # Gère les erreurs de DB (ex: violation de contrainte si le vélo est utilisé ailleurs)
        await db.rollback()
        # On pourrait être plus précis sur les codes d'erreur PostgreSQL ici.
        raise HTTPException(status_code=500, detail=f"Database error during deletion: {e}")
        
    return # Réponse 204 No Content (succès sans contenu à retourner)

# =================================================================
# 4. ROUTER FAITS (Lecture d'état et Suppression Ciblée)
# (Faits asynchrones, Suppression Sécurisée)
# =================================================================

@router_facts.get("/stations/{station_code}/etat_actuel", response_model=EtatStation, summary="État le plus récent d'une station")
async def read_station_current_state(station_code: str, db: AsyncSession = Depends(get_db)):
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

@router_facts.delete("/localisations/{loc_id}", 
                     status_code=status.HTTP_204_NO_CONTENT, 
                     summary="Supprime une ligne de localisation par ID",
                     dependencies=[Depends(api_key_auth)]) # SÉCURISÉ
async def delete_location(loc_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("DELETE FROM localisation_velo WHERE loc_id = :id RETURNING loc_id;"), {"id": loc_id})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Location ID not found")
    await db.commit()
    return

# =================================================================
# 5. ROUTER ANALYSE (Trajets et Statistiques)
# (Lecture asynchrone, Path Parameter)
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


@router_analysis.get("/trajets/velo/{velo_name}", response_model=List[Trajet], summary="Liste des trajets d'un vélo spécifique")
async def read_trajets_by_velo(
    velo_name: str, # Path Parameter
    db: AsyncSession = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    sql_query = text(f"""
        SELECT velo_name, station_depart_code, heure_depart, station_arrivee_code, heure_arrivee, duree_trajet_minutes
        FROM V_TRAJETS
        WHERE velo_name = :velo_name
        ORDER BY heure_depart DESC
        LIMIT :limit OFFSET :offset;
    """)
    
    params = {"velo_name": velo_name, "limit": limit, "offset": offset}
    result = await db.execute(sql_query, params)
    rows = result.all()
    return [Trajet.model_validate(row, from_attributes=True) for row in rows]




@router_analysis.get("/trajets/top_routes", response_model=List[TrajetStats], summary="Top 10 des trajets les plus populaires (filtrable temporellement)")
async def get_top_routes(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, ge=1, le=100),
    start_date: Optional[dt.date] = Query(None, description="Date de début (YYYY-MM-DD)"),
    end_date: Optional[dt.date] = Query(None, description="Date de fin (YYYY-MM-DD)")
):
    where_clause = []
    params = {"limit": limit}
    
    if start_date:
        where_clause.append("heure_depart >= :start_date")
        params['start_date'] = start_date
        
    if end_date:
        # Ajout d'une journée pour inclure toute la journée de fin
        where_clause.append("heure_depart < :end_date + INTERVAL '1 day'")
        params['end_date'] = end_date
        
    where_sql = f"WHERE {' AND '.join(where_clause)}" if where_clause else ""

    sql_query = text(f"""
        SELECT 
            station_depart_code,
            station_arrivee_code,
            COUNT(*) AS nombre_trajets,
            AVG(duree_trajet_minutes) AS duree_moyenne_minutes
        FROM V_TRAJETS
        {where_sql}
        GROUP BY 1, 2
        ORDER BY nombre_trajets DESC
        LIMIT :limit;
    """)
    
    result = await db.execute(sql_query, params)
    rows = result.all()
    return [TrajetStats.model_validate(row, from_attributes=True) for row in rows]



@router_analysis.get("/velos/{velo_name}/stations_visitees", response_model=VeloStationsCount, summary="Nombre de stations distinctes visitées par un vélo sur une période.")
async def get_velo_stations_count(
    velo_name: str,
    db: AsyncSession = Depends(get_db),
    periode: str = Query("mois", description="Période à analyser : 'jour', 'semaine', 'mois', 'annee'."),
    date_ref: Optional[dt.date] = Query(None, description="Date de référence (YYYY-MM-DD). Par défaut, la date d'aujourd'hui.")
):
    date_reference = date_ref if date_ref else dt.date.today()
    
    # Mappage pour construire la clause WHERE
    periode_map = {
        "jour": "DATE(heure_depart) = :date_ref",
        "semaine": "DATE(heure_depart) >= :date_ref - INTERVAL '7 days' AND DATE(heure_depart) <= :date_ref",
        "mois": "DATE(heure_depart) >= :date_ref - INTERVAL '1 month' AND DATE(heure_depart) <= :date_ref",
        "annee": "DATE(heure_depart) >= :date_ref - INTERVAL '1 year' AND DATE(heure_depart) <= :date_ref",
    }
    
    if periode.lower() not in periode_map:
        raise HTTPException(status_code=400, detail="Période invalide. Utilisez 'jour', 'semaine', 'mois', ou 'annee'.")
        
    where_condition = periode_map[periode.lower()]

    sql_query = text(f"""
        WITH VisitedStations AS (
            -- On combine les stations de départ et d'arrivée, en assurant l'unicité
            SELECT station_depart_code AS station_code FROM V_TRAJETS
            WHERE velo_name = :vn AND {where_condition}
            UNION
            SELECT station_arrivee_code AS station_code FROM V_TRAJETS
            WHERE velo_name = :vn AND {where_condition}
        )
        SELECT COUNT(station_code) FROM VisitedStations;
    """)
    
    params = {"vn": velo_name, "date_ref": date_reference}
    
    count_result = await db.execute(sql_query, params)
    count_value = count_result.scalar_one()
    
    return VeloStationsCount.model_validate({
        "velo_name": velo_name,
        "periode": periode.lower(),
        "nombre_stations_visitees": count_value
    }, from_attributes=True)

@router_analysis.get("/velos/{velo_name}/boomerang", summary="Vérifie si un vélo a effectué un trajet boomerang")
async def check_boomerang(velo_name: str, db: AsyncSession = Depends(get_db)):
    sql_query = text("""
        SELECT COUNT(*)
        FROM V_TRAJETS
        WHERE velo_name = :vn
        AND station_depart_code = station_arrivee_code;
    """)
    
    count_result = await db.execute(sql_query, {"vn": velo_name})
    count_value = count_result.scalar_one()
    
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


@router_analysis.get("/stations/flow_imbalance", response_model=List[StationFlowImbalance], summary="Déséquilibre de flux (Départs - Arrivées) sur une période")
async def get_flow_imbalance(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
    start_date: Optional[dt.date] = Query(None, description="Date de début (YYYY-MM-DD)"),
    end_date: Optional[dt.date] = Query(None, description="Date de fin (YYYY-MM-DD)")
):
    where_clause = []
    params = {"limit": limit}

    if start_date:
        where_clause.append("heure_depart >= :start_date")
        params['start_date'] = start_date

    if end_date:
        where_clause.append("heure_depart < :end_date + INTERVAL '1 day'")
        params['end_date'] = end_date
        
    where_sql = f"WHERE {' AND '.join(where_clause)}" if where_clause else ""
    
    sql_query = text(f"""
        WITH FilteredTrajets AS (
            SELECT * FROM V_TRAJETS
            {where_sql}
        ),
        Departures AS (
            SELECT station_depart_code AS code, COUNT(*) AS departures FROM FilteredTrajets GROUP BY 1
        ),
        Arrivals AS (
            SELECT station_arrivee_code AS code, COUNT(*) AS arrivals FROM FilteredTrajets GROUP BY 1
        )
        -- Le reste de la requête d'agrégation reste inchangé...
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
    
    result = await db.execute(sql_query, params)
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