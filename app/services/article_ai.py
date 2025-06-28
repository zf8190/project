import os
import json
import logging
from typing import List, Union

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_

from app.models.team import Team
from app.models.article import Article
from app.models.feed import Feed

from openai import AsyncOpenAI

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = "gpt-3.5-turbo"

logger = logging.getLogger("ArticleAIProcessor")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class ArticleAIProcessor:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _mark_feeds_as_processed(self, feeds: List[Feed]):
        for feed in feeds:
            feed.processed = True
            self.db.add(feed)
        try:
            await self.db.commit()
        except Exception as e:
            logger.error(f"Errore durante il salvataggio dei feed marcati come processed: {e}")
            await self.db.rollback()

    def _normalize_str(self, value: Union[str, List[str], None]) -> str:
        if isinstance(value, list):
            return "\n".join(str(v) for v in value)
        if value is None:
            return ""
        return str(value)

    async def process_all_teams(self):
        try:
            teams = (await self.db.execute(select(Team))).scalars().all()
        except Exception as e:
            logger.error(f"Errore nel caricamento delle squadre: {e}")
            return

        for team in teams:
            try:
                article = await self._get_article_for_team(team.id)
                new_feeds = await self._get_unprocessed_feeds_for_team(team.id)

                if not article and not new_feeds:
                    logger.info(f"[Team {team.name}] Nessun articolo e nessun feed nuovo. Passo al prossimo team.")
                    continue

                if not article and new_feeds:
                    logger.info(f"[Team {team.name}] Nessun articolo ma feed nuovi trovati. Generazione articolo ex novo.")
                    await self._generate_new_article(team, new_feeds)
                    continue

                if article and not new_feeds:
                    logger.info(f"[Team {team.name}] Articolo esiste, nessun feed nuovo. Nessun aggiornamento necessario.")
                    continue

                if article and new_feeds:
                    logger.info(f"[Team {team.name}] Articolo esiste e feed nuovi trovati. Aggiornamento articolo.")
                    await self._update_existing_article(article, new_feeds)

            except Exception as e:
                logger.error(f"[Team {team.name}] Errore durante il processamento: {e}")
                await self.db.rollback()

    async def _get_article_for_team(self, team_id: int):
        result = await self.db.execute(select(Article).where(Article.team_id == team_id))
        return result.scalars().first()

    async def _get_unprocessed_feeds_for_team(self, team_id: int) -> List[Feed]:
        result = await self.db.execute(
            select(Feed).where(Feed.team_id == team_id, Feed.processed == False)
        )
        return result.scalars().all()

    async def _parse_openai_response(self, raw_content: str, team_name: str) -> dict:
        try:
            data = json.loads(raw_content)
            if isinstance(data, list):
                if len(data) > 0 and isinstance(data[0], dict):
                    data = data[0]
                else:
                    logger.warning(f"[Team {team_name}] OpenAI response is a list but does not contain dict, using empty dict.")
                    data = {}
            elif not isinstance(data, dict):
                logger.warning(f"[Team {team_name}] OpenAI response is not a dict, got {type(data)}, using empty dict.")
                data = {}
            return data
        except json.JSONDecodeError as e:
            logger.error(f"[Team {team_name}] JSONDecodeError parsing OpenAI response: {e}")
            return {}

    async def _generate_new_article(self, team: Team, feeds: List[Feed]):
        combined_text = "\n\n".join([f"Titolo: {f.title}\nTesto: {f.content}" for f in feeds])
        prompt = (
            "Sei un giornalista sportivo esperto di calciomercato.\n"
            "Leggi questi feed di calciomercato e scrivi un articolo lungo e articolato basandoti esclusivamente su questi feed.\n"
            "Cerca di non essere ripetitivo ma accorpa in frasi le notizie su calciatori/eventi simili.\n"
            "Separa le diverse frasi con l'espressione <br>.\n"
            f"Feed:\n{combined_text}\n\n"
            "Rispondi esclusivamente con un singolo oggetto JSON valido, senza testo aggiuntivo o spiegazioni, "
            "nel formato {'title': ..., 'content': ...}."
        )
        try:
            response = await client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1500,
            )
            raw_content = response.choices[0].message.content
            data = await self._parse_openai_response(raw_content, team.name)
            logger.info(f"[Team {team.name}] Articolo generato con successo.")
            await self._mark_feeds_as_processed(feeds)

        except Exception as e:
            logger.error(f"[Team {team.name}] Errore OpenAI durante generazione articolo: {e}")
            data = {"title": f"Aggiornamenti {team.name}", "content": "Errore nella generazione dell'articolo."}

        try:
            title = self._normalize_str(data.get("title", f"Aggiornamenti {team.name}"))
            content = self._normalize_str(data.get("content", ""))
            new_article = Article(
                team_id=team.id,
                title=title,
                content=content,
            )
            self.db.add(new_article)
            await self.db.commit()
            logger.info(f"[Team {team.name}] Articolo salvato correttamente.")
        except Exception as e:
            logger.error(f"[Team {team.name}] Errore durante il salvataggio articolo: {e}")
            await self.db.rollback()

    async def _update_existing_article(self, article: Article, feeds: List[Feed]):
        combined_new_text = "\n\n".join([f"Titolo: {f.title}\nTesto: {f.content}" for f in feeds])
        prompt = (
            "Sei un giornalista sportivo esperto di calciomercato.\n"
            "Leggi questi feed nuovi di calciomercato e scrivi un articolo lungo e articolato basandoti esclusivamente su questi feed e su quelli accorpati prima,\n"
            "Cerca di non essere ripetitivo ma accorpa in frasi le notizie su calciatori/eventi simili,\n"
            "Non fare titoli sensazionalistici ed evita di citare le fonti,\n"
            "Separa gli argomenti diversi con l'espressione '<br>'.\n"
            f"feed_accorpati:\n{article.content}\n\n"
            f"feed_nuovi:\n{combined_new_text}\n\n"
            "Rispondi esclusivamente con un singolo oggetto JSON valido, senza testo aggiuntivo o spiegazioni, "
            "nel formato {'title': ..., 'content': ...}."
        )
        try:
            response = await client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1500,
            )
            raw_content = response.choices[0].message.content
            data = await self._parse_openai_response(raw_content, f"team_id {article.team_id}")
            logger.info(f"[Team {article.team_id}] Articolo aggiornato con successo.")
            await self._mark_feeds_as_processed(feeds)

        except Exception as e:
            logger.error(f"[Team {article.team_id}] Errore OpenAI durante aggiornamento articolo: {e}")
            data = {"title": article.title, "content": article.content}

        try:
            article.title = self._normalize_str(data.get("title", article.title))
            article.content = self._normalize_str(data.get("content", article.content))
            await self.db.commit()
            logger.info(f"[Team {article.team_id}] Articolo aggiornato salvato correttamente.")
        except Exception as e:
            logger.error(f"[Team {article.team_id}] Errore durante il salvataggio aggiornamento articolo: {e}")
            await self.db.rollback()

    async def cleanup_feeds(self):
        """
        - Elimina i feed processed = False con team associati (team_id not None).
        - Reimposta processed = False per i feed processed = True con team associati.
        """
        try:
            # Elimina feed processed=False con team (team_id is not None)
            delete_stmt = delete(Feed).where(
                and_(
                    Feed.processed == False,
                    Feed.team_id != None,
                    Feed.team_id != 0
                )
            )
            await self.db.execute(delete_stmt)

            # Reimposta processed=False per feed processed=True con team associati (team_id not None)
            update_stmt = update(Feed).where(
                and_(
                    Feed.processed == True,
                    Feed.team_id != None,
                    Feed.team_id != 0
                )
            ).values(processed=True)
            await self.db.execute(update_stmt)

            await self.db.commit()
            logger.info("Cleanup feed completato: eliminati feed senza team, resettati feed processed con team.")
        except Exception as e:
            logger.error(f"Errore durante cleanup feed: {e}")
            await self.db.rollback()
