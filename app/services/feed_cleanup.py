import datetime
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.feed import Feed
from newspaper import Article
import logging
import requests

logger = logging.getLogger("FeedEnricher")

def extract_original_url(url: str) -> str:
    """
    Se l'URL è un link Google News RSS redirect, prova a risolvere il link originale.
    Altrimenti ritorna l'URL così com'è.
    """
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        # Se la risposta finale è diversa dall'url iniziale, prendiamo quella
        final_url = response.url
        # Escludi URL che ancora contengono 'news.google.com/rss' o simili
        if "news.google.com/rss" in final_url or final_url == url:
            return url
        return final_url
    except Exception as e:
        logger.warning(f"Impossibile risolvere redirect per {url}: {e}")
        return url

async def fetch_article_content(url: str) -> str:
    try:
        # Provo a estrarre il link originale
        original_url = extract_original_url(url)

        article = Article(original_url)
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
