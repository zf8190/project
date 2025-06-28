#article_extractor.py
import logging
import requests
from newspaper import Article
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.feed import Feed

logger = logging.getLogger("FeedContentFetcher")

class FeedContentFetcher:
    """
    Classe incaricata di arricchire i feed non processati,
    che hanno un team associato e non hanno contenuto.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def enrich_feed_content(self) -> int:
        """
        Cerca tutti i feed non processati, con team associato e senza content.
        Tenta di risolvere il link dell'articolo e salvare il contenuto.

        :return: numero di feed aggiornati con contenuto.
        """
        result = await self.db.execute(
            select(Feed).where(
                Feed.processed == False,
                Feed.team_id.isnot(None),
                (Feed.content == None) | (Feed.content == "")
            )
        )
        feeds = result.scalars().all()

        updated_count = 0

        for feed in feeds:
            try:
                resolved_url = self.resolve_final_url(feed.link)
                content = self.extract_article_content(resolved_url)
                if content and len(content) > 100:
                    feed.content = content
                    feed.processed = True
                    self.db.add(feed)
                    updated_count += 1
            except Exception as e:
                logger.warning(f"Errore su feed ID {feed.id}: {e}")

        if updated_count > 0:
            try:
                await self.db.commit()
            except Exception as e:
                logger.error(f"Errore durante il commit: {e}")
                await self.db.rollback()
                return 0

        return updated_count

    def resolve_final_url(self, url: str) -> str:
        """
        Usa requests.head per seguire eventuali redirect e ottenere l'URL finale.
        """
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            final_url = response.url
            return final_url
        except Exception as e:
            logger.warning(f"Impossibile risolvere redirect per {url}: {e}")
            return url

    def extract_article_content(self, url: str) -> str:
        """
        Usa newspaper per scaricare e parsare il contenuto testuale dell'articolo.
        """
        try:
            article = Article(url)
            article.download()
            article.parse()
            return article.text
        except Exception as e:
            logger.error(f"Errore estraendo contenuto da {url}: {e}")
            return ""
