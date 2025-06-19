from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import asyncio

from app.db import async_session
from app.services.feed_ingestion import ingest_feeds
from app.services.feed_association import FeedTeamAssociatorAI
from app.services.article_ai import process_all_teams_articles

scheduler = AsyncIOScheduler()

def schedule_jobs():
    scheduler.add_job(
        lambda: asyncio.create_task(feed_ingestion_job()),
        trigger=CronTrigger(minute="0,30", hour="8-22"),
        id="feed_ingestion_job",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: asyncio.create_task(feed_association_job()),
        trigger=CronTrigger(minute="5,35", hour="8-22"),
        id="feed_association_job",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: asyncio.create_task(process_all_teams_articles_job()),
        trigger=CronTrigger(minute="15,45", hour="8-22"),
        id="process_all_teams_articles_job",
        replace_existing=True,
    )

async def feed_ingestion_job():
    print(f"[{datetime.now()}] Starting feed ingestion job...")
    async with async_session() as db:
        await ingest_feeds(db)
    print(f"[{datetime.now()}] Feed ingestion job completed.")

async def feed_association_job():
    print(f"[{datetime.now()}] Starting feed association job...")
    async with async_session() as db:
        associator = FeedTeamAssociatorAI(db)
        await associator.associate_feeds()
    print(f"[{datetime.now()}] Feed association job completed.")

async def process_all_teams_articles_job():
    print(f"[{datetime.now()}] Starting process all teams articles job...")
    async with async_session() as db:
        await process_all_teams_articles(db)
    print(f"[{datetime.now()}] Process all teams articles job completed.")
