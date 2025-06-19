from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy import text

import os

from app.db import get_db, get_engine
from app.models.article import Article
from app.models.team import Team
from app.models.base import Base
from app.config import STATIC_URL
from app.api.jobs import router as jobs_router


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    db_url = os.getenv("DATABASE_URL", "❌ DATABASE_URL non trovato")
    print(f"🔧 Stringa di connessione al DB: {db_url}")

    try:
        async with get_engine().connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            print("✅ Connessione al database riuscita! Risultato:", result.scalar())
    except Exception as e:
        print("❌ Errore nella connessione al database:", e)

    try:
        async with get_engine().begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(feed_per_teams.create, checkfirst=True)
        print("✅ Tabelle del database create (se non esistevano)")
    except Exception as e:
        print("❌ Errore nella creazione delle tabelle:", e)

# Include router e static
app.include_router(jobs_router, prefix="/api")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def read_home(request: Request, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Article)
        .join(Article.team)  # join esplicito per poter ordinare su Team.name
        .options(joinedload(Article.team))  # carica la relazione team
        .order_by(Team.name)
    )
    result = await db.execute(stmt)
    articles = result.scalars().all()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "articles": articles,
            "STATIC_URL": STATIC_URL
        }
    )


@app.get("/team/{team_name}", response_class=HTMLResponse)
async def read_article(team_name: str, request: Request, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Article)
        .join(Article.team)
        .options(joinedload(Article.team))  # eager load anche qui per evitare lazy load error
        .where(Team.name.ilike(team_name))
    )
    result = await db.execute(stmt)
    article = result.scalars().first()

    if not article:
        raise HTTPException(status_code=404, detail="Articolo non trovato")

    return templates.TemplateResponse(
        "article.html",
        {
            "request": request,
            "article": article,
            "STATIC_URL": STATIC_URL
        }
    )
