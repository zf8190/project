# app/db.py

import os
import ssl
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Carica .env se presente
load_dotenv()

# Leggi la DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL non impostato nelle variabili ambiente")

# Converti URL per asyncpg
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Configura SSL per Railway
connect_args = {}
if "railway" in DATABASE_URL:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    connect_args = {"ssl": ssl_context}

# Crea il motore async
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, connect_args=connect_args)

# Session factory
async_session = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

# Dependency FastAPI
async def get_db():
    async with async_session() as session:
        yield session

# Export engine
def get_engine():
    return engine
