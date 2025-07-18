import feedparser
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.feed import Feed
from app.feed_config.feed_team_map import FEED_TEAM_MAP  # ✅ ora unica fonte
from app.services.feed_cleanup import sgr_ezza_feeds
import datetime

MAX_LEN = 1024

logger = logging.getLogger("feed_ingestion")
logger.setLevel(logging.INFO)

def truncate_string(s: str, max_len: int = MAX_LEN) -> str:
    if s is None:
        return ""
    return s[:max_len]

async def ingest_feeds(db: AsyncSession):
    new_count = 0

    for rss_url, team_id in FEED_TEAM_MAP.items():
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

                content = ""
                if "content" in entry and entry["content"]:
                    content = entry["content"][0].get("value", "")

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
                    team_id=team_id  # ✅ già noto dalla mappa
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
    
    try:
        sgrezzati = await sgr_ezza_feeds(db)
        logger.info(f"[FeedIngestion] Sgrezzati {sgrezzati} feed più vecchi di 24h.")
    except Exception as e:
        logger.error(f"[FeedIngestion] Errore durante sgr_ezza_feeds: {e}")
