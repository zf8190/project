# app/services/article_ai.py

import os
import json
import logging
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.team import Team
from app.models.article import Article
from app.models.feed import Feed

from openai import AsyncOpenAI

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = "gpt-3.5-turbo"

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
            "Crea un articolo originale e dettagliato basandoti esclusivamente sui seguenti feed.\n"
            "Non sintetizzare, ma riorganizza le informazioni in modo chiaro.\n"
            f"Feed:\n{combined_text}\n\n"
            "Rispondi in JSON con due campi: 'title' e 'content'."
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
            "Aggiorna questo articolo integrando le nuove informazioni, "
            "mantenendo e aggiornando tutte le informazioni utili già presenti.\n"
            f"Articolo esistente:\n{article.content}\n\n"
            f"Nuove notizie:\n{combined_new_text}\n\n"
            "Rispondi in JSON con 'title' e 'content' aggiornati."
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
            await self.db.rollback()
