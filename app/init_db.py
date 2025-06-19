# init_db.py

import asyncio
from app.db import get_engine
from app.models.base import Base

async def init_db():
    engine = get_engine()
    async with engine.begin() as conn:
        print("🔧 Creo le tabelle nel database se non esistono...")
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tabelle create o già esistenti.")

if __name__ == "__main__":
    asyncio.run(init_db())
