from fastapi import APIRouter, HTTPException, BackgroundTasks
from datetime import datetime

from app.scheduler import (
    feed_ingestion_job,
    feed_association_job,
    process_all_teams_articles_job,
    cleanup_feeds_job,
    enrich_feed_contents_job,  # Importa il nuovo job
)

router = APIRouter()

@router.get("/jobs/feed-ingestion")
async def run_feed_ingestion_job(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(feed_ingestion_job)
        return {"status": "started", "job": "feed_ingestion_job", "started_at": str(datetime.utcnow())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/jobs/feed-association")
async def run_feed_association_job(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(feed_association_job)
        return {"status": "started", "job": "feed_association_job", "started_at": str(datetime.utcnow())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/jobs/process-articles")
async def run_process_all_teams_articles_job(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(process_all_teams_articles_job)
        return {"status": "started", "job": "process_all_teams_articles_job", "started_at": str(datetime.utcnow())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/jobs/cleanup-feeds")
async def run_cleanup_feeds_job(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(cleanup_feeds_job)
        return {"status": "started", "job": "cleanup_feeds_job", "started_at": str(datetime.utcnow())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/jobs/enrich-feed-content")
async def run_enrich_feed_content_job(background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(enrich_feed_contents_job)
        return {"status": "started", "job": "enrich_feed_contents_job", "started_at": str(datetime.utcnow())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
