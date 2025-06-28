import datetime
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.feed import Feed
from newspaper import Article
import logging

logger = logging.getLogger("FeedEnricher")

async def fetch_article_content(url: str) -> str:
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        logger.error(f"Errore estrazione articolo da {url}: {e}")
        return ""

async def sgr_ezza_feeds(db: AsyncSession) -> int:
    """
    Aggiorna a processed=True tutti i feed non processati con published_at più vecchio di 24 ore,
    arricchendo il campo content con il testo estratto dall'url.

    :param db: sessione DB asincrona
    :return: numero di feed aggiornati
    """
    now = datetime.datetime.utcnow()
    cutoff = now - datetime.timedelta(hours=24)

    result = await db.execute(
        select(Feed).where(
            Feed.processed == False,
            Feed.published_at < cutoff
        )
    )
    feeds_to_update = result.scalars().all()

    count = 0
    for feed in feeds_to_update:
        # Estraggo il contenuto solo se content è vuoto o troppo corto
        if not feed.content or len(feed.content) < 100:
            content = await fetch_article_content(feed.link)
            if content:
                feed.content = content
        
        feed.processed = True
        db.add(feed)
        count += 1

    if count > 0:
        try:
            await db.commit()
        except Exception as e:
            logger.error(f"Errore durante commit aggiornamento feeds: {e}")
            await db.rollback()
            return 0

    return count
