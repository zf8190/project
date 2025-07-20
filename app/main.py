from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
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
from app.scheduler import scheduler, schedule_jobs


app = FastAPI()

# 🔁 Middleware: Redirect da top10market.it a www.top10market.it
@app.middleware("http")
async def redirect_root_domain(request: Request, call_next):
    host = request.headers.get("host")
    if host == "top10market.it":
        new_url = f"https://www.top10market.it{request.url.path}"
        if request.url.query:
            new_url += f"?{request.url.query}"
        return RedirectResponse(url=new_url, status_code=301)
    return await call_next(request)

# 🚀 Startup: connessione DB + scheduler
@app.on_event("startup")
async def startup_event():
    db_url = os.getenv("DATABASE_URL", "❌ DATABASE_URL non trovato")
    print(f"🔧 Stringa di connessione al DB: {db_url}")

    # ✅ Test DB connection
    try:
        async with get_engine().connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            print("✅ Connessione al database riuscita! Risultato:", result.scalar())
    except Exception as e:
        print("❌ Errore nella connessione al database:", e)

    # ✅ Crea tabelle se non esistono
    try:
        async with get_engine().begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("✅ Tabelle del database create (se non esistevano)")
    except Exception as e:
        print("❌ Errore nella creazione delle tabelle:", e)

    # ✅ Avvio scheduler
    schedule_jobs()
    scheduler.start()
    print("🚀 Scheduler avviato con job:", scheduler.get_jobs())

# 📦 Static & router
app.include_router(jobs_router, prefix="/api")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# 🏠 Home page
@app.get("/", response_class=HTMLResponse)
async def read_home(request: Request, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Article)
        .join(Article.team)
        .options(joinedload(Article.team))
        .order_by(Article.team_id)
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

# 📄 Articolo per team
@app.get("/team/{team_name}", response_class=HTMLResponse)
async def read_article(team_name: str, request: Request, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Article)
        .join(Article.team)
        .options(joinedload(Article.team))
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
