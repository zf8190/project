# app/services/article_ai.py

import os
import json
import logging
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, or_

from app.models.team import Team
from app.models.article import Article
from app.models.feed import Feed

from openai import AsyncOpenAI

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = "gpt-4"

logger = logging.getLogger("ArticleAIProcessor")
logger.setLevel(logging.INFO)
# Configura un handler base, lo puoi personalizzare nel main
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class ArticleAIProcessor:
    def __init__(self, db: AsyncSession):
        self.db = db

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

    async def _generate_new_article(self, team: Team, feeds: List[Feed]):
        combined_text = "\n\n".join([f"Titolo: {f.title}\nTesto: {f.content}" for f in feeds])
        prompt = (
            "Sei un giornalista sportivo esperto di calciomercato.\n"
            "Leggi questi feed di calciomercato e scrivi un articolo lungo e articolato basandoti esclusivamenete su questi feed\n"
            "Suddividi l'articolo in diversi punti, elaborando e accorpando le varie notizie sullo stesso giocatore/evento senza essere ripetitivo \n"
            "L'articolo che stai scrivendo avrà quindi diversi paragrafi relativi allo stesso giocatore/evento\n"
            "Delimita ogni paragrafo andando a capo con l'espressione <br> \n"
            f"Feed:\n{combined_text}\n\n"
            "Rispondi esclusivamente con un oggetto JSON valido, senza testo aggiuntivo o spiegazioni nel formato 'title' e 'content'."
        )
        try:
            response = await client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1000,
            )
            data = json.loads(response.choices[0].message.content)
            logger.info(f"[Team {team.name}] Articolo generato con successo.")
        except Exception as e:
            logger.error(f"[Team {team.name}] Errore OpenAI durante generazione articolo: {e}")
            data = {"title": f"Aggiornamenti {team.name}", "content": "Errore nella generazione dell'articolo."}

        try:
            new_article = Article(
                team_id=team.id,
                title=data.get("title", f"Aggiornamenti {team.name}"),
                content=data.get("content", ""),
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
            "Leggi questi feed  e l'articolo che avevi già scritto precedentemente di calciomercato. scrivi un articolo lungo e articolato basandoti esclusivamenete su questi feed e l'articolo\n"
            "Suddividi l'articolo in diversi punti, elaborando e accorpando le varie notizie sullo stesso giocatore/evento senza essere ripetitivo \n"
            "L'articolo che stai scrivendo avrà quindi diversi paragrafi relativi allo stesso giocatore/evento\n"
            "Delimita ogni paragrafo andando a capo con l'espressione <br> \n"
            f"Articolo esistente:\n{article.content}\n\n"
            f"Nuove notizie:\n{combined_new_text}\n\n"
            "Rispondi esclusivamente con un oggetto JSON valido, senza testo aggiuntivo o spiegazioni nel formato 'title' e 'content'."
        )
        try:
            response = await client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1000,
            )
            data = json.loads(response.choices[0].message.content)
            logger.info(f"[Team {article.team_id}] Articolo aggiornato con successo.")
        except Exception as e:
            logger.error(f"[Team {article.team_id}] Errore OpenAI durante aggiornamento articolo: {e}")
            data = {"title": article.title, "content": article.content}

        try:
            article.title = data.get("title", article.title)
            article.content = data.get("content", article.content)
            await self.db.commit()
            logger.info(f"[Team {article.team_id}] Articolo aggiornato salvato correttamente.")
        except Exception as e:
            logger.error(f"[Team {article.team_id}] Errore durante il salvataggio aggiornamento articolo: {e}")
            logger.debug(f"[Team {article.team_id}] Contenuto title fallito: {data.get('title')}")
            logger.debug(f"[Team {article.team_id}] Contenuto content fallito: {data.get('content')}")
            await self.db.rollback()

    async def cleanup_feeds(self):
        """
        - Elimina i feed processed = True senza team associati (team_id is None).
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
            ).values(processed=False)
            await self.db.execute(update_stmt)

            await self.db.commit()
            logger.info("Cleanup feed completato: eliminati feed senza team, resettati feed processed con team.")
        except Exception as e:
            logger.error(f"Errore durante cleanup feed: {e}")
            await self.db.rollback()
