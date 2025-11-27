from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase # Typiquement nécessaire pour les modèles
import os
from dotenv import load_dotenv

# --- 1. Configuration de l'environnement ---

load_dotenv() # Charge les variables .env

# Construction de l'URL de la base de données
# L'utilisation de 'postgresql+asyncpg' est correcte pour l'asynchrone
DATABASE_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# --- 2. Configuration de l'Engine et de la Session Asynchrone ---

engine = create_async_engine(
     DATABASE_URL,
    echo=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    autoflush=False,
    expire_on_commit= False,
)

# --- Déclaration de base SQLAlchemy (Cruciale pour l'ORM) ---
class Base(DeclarativeBase):
    pass

# --- 3. Dépendance FastAPI pour la gestion des Sessions Asynchrones ---

async def get_db() -> AsyncSession:
  """
  Dépendance asynchrone pour fournir une session DB (AsyncSession)
  aux gestionnaires de routes (FastAPI dependency).
  """
  async with AsyncSessionLocal() as session:
    try:
        yield session
    finally:
            await session.close() # S'assurer que la session est fermée

# --- 4. Initialisation de l'Application FastAPI ---

app = FastAPI(title="Vélib Analytics API")



