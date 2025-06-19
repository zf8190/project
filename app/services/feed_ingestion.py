# app/services/feed_ingestion.py

import feedparser
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.feed import Feed
from app.config import load_rss_feeds
import datetime

MAX_LEN = 255

logger = logging.getLogger("feed_ingestion")
logger.setLevel(logging.INFO)

def truncate_string(s: str, max_len: int = MAX_LEN) -> str:
    if s is None:
        return ""
    return s[:max_len]

async def ingest_feeds(db: AsyncSession):
    rss_urls = load_rss_feeds()
    new_count = 0

    for rss_url in rss_urls:
        try:
            d = feedparser.parse(rss_url)
            feed_source = truncate_string(rss_url)
        except Exception as e:
            logger.error(f"[FeedIngestion] Errore nel parsing RSS URL {rss_url}: {e}")
            continue

        for entry in d.entries:
            try:
                feed_entry_id = truncate_string(getattr(entry, "id", None) or getattr(entry, "link", ""))
                if not feed_entry_id:
                    logger.warning(f"[FeedIngestion] Entry senza id/link in feed {rss_url}, skip.")
                    continue

                # Evita duplicati
                result = await db.execute(select(Feed).where(Feed.feed_entry_id == feed_entry_id))
                existing = result.scalars().first()
                if existing:
                    continue

                title = truncate_string(getattr(entry, "title", ""))
                link = truncate_string(getattr(entry, "link", ""))
                summary = truncate_string(getattr(entry, "summary", ""))
                content = entry.get("content", [{"value": ""}])[0]["value"]

                try:
                    published_at = datetime.datetime(*entry.published_parsed[:6])
                except Exception:
                    published_at = datetime.datetime.utcnow()

                new_feed = Feed(
                    feed_source=feed_source,
                    feed_entry_id=feed_entry_id,
                    title=title,
                    link=link,
                    summary=summary,
                    content=content,
                    published_at=published_at,
                    processed=False,
                    team_id=None  # temporaneo nullable
                )

                db.add(new_feed)
                new_count += 1

            except Exception as e:
                logger.error(f"[FeedIngestion] Errore durante il processing entry {feed_entry_id} da {rss_url}: {e}")
                continue

    try:
        await db.commit()
        logger.info(f"[FeedIngestion] Inseriti {new_count} nuovi feed.")
    except Exception as e:
        logger.error(f"[FeedIngestion] Errore durante commit DB: {e}")
