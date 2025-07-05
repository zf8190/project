from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import asyncio

from app.db import async_session
from app.services.feed_ingestion import ingest_feeds
from app.services.feed_association import FeedTeamAssociatorAI
from app.services.article_ai import ArticleAIProcessor
from app.services.article_extractor import FeedContentFetcher  # Import della nuova classe

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
        next_run_time=None,  # Non esegue subito il job
    )
    scheduler.add_job(
        lambda: asyncio.create_task(process_all_teams_articles_job()),
        trigger=CronTrigger(minute="5,35", hour="8-22"),
        id="process_all_teams_articles_job",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: asyncio.create_task(cleanup_feeds_job()),
        trigger=CronTrigger(minute="10,40", hour="8-22"),
        id="cleanup_feeds_job",
        replace_existing=True,
        next_run_time=None,  # Non esegue subito il job
    )
    scheduler.add_job(
        lambda: asyncio.create_task(enrich_feed_contents_job()),  # Nuovo job
        trigger=CronTrigger(minute="20,50", hour="8-22"),
        id="enrich_feed_contents_job",
        replace_existing=True,
        next_run_time=None,  # Non esegue subito il job
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
        processor = ArticleAIProcessor(db)
        await processor.process_all_teams()
    print(f"[{datetime.now()}] Process all teams articles job completed.")


async def cleanup_feeds_job():
    print(f"[{datetime.now()}] Starting cleanup feeds job...")
    async with async_session() as db:
        processor = ArticleAIProcessor(db)
        await processor.cleanup_feeds()
    print(f"[{datetime.now()}] Cleanup feeds job completed.")


async def enrich_feed_contents_job():
    print(f"[{datetime.now()}] Starting enrich feed contents job...")
    async with async_session() as db:
        fetcher = FeedContentFetcher(db)
        updated = await fetcher.enrich_feed_content()
        print(f"Updated {updated} feeds with content.")
    print(f"[{datetime.now()}] Enrich feed contents job completed.")
