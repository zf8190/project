# app/services/feed_association.py

import os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.feed import Feed
from app.services.team_service import get_all_teams
from openai import AsyncOpenAI

class FeedTeamAssociatorAI:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-3.5-turbo"

    async def _get_unassigned_unprocessed_feeds(self):
        result = await self.db.execute(
            select(Feed).where(Feed.team_id == None, Feed.processed == False)
        )
        return result.scalars().all()

    async def associate_feeds(self):
        feeds = await self._get_unassigned_unprocessed_feeds()
        if not feeds:
            print("[FeedTeamAssociatorAI] Nessun feed non associato e non processato trovato.")
            return

        teams = await get_all_teams(self.db)
        team_names = [team.name for team in teams]

        for feed in feeds:
            prompt = (
                "Sei un assistente che associa un feed di notizie sportive a uno dei seguenti team: "
                f"{', '.join(team_names)}.\n"
                "Leggi questo feed:\n"
                f"Titolo: {feed.title}\n"
                f"Contenuto: {feed.content}\n\n"
                "Rispondi solo con il nome del team a cui associare questo feed, oppure 'None' se nessun team è rilevante."
            )

            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                    max_tokens=10,
                )
                team_name_ai = response.choices[0].message.content.strip()
            except Exception as e:
                print(f"[{feed.id}] Errore AI durante associazione team: {e}")
                continue

            if team_name_ai == "None" or team_name_ai not in team_names:
                # Segna come processato senza team
                try:
                    feed.processed = True
                    await self.db.commit()
                    print(f"[{feed.id}] Feed marcato come processato senza team.")
                except Exception as e:
                    print(f"[{feed.id}] Errore aggiornamento feed processato: {e}")
                    await self.db.rollback()
                continue

            team_obj = next((t for t in teams if t.name == team_name_ai), None)
            if not team_obj:
                print(f"[{feed.id}] Team '{team_name_ai}' non trovato nella lista team.")
                continue

            try:
                feed.team_id = team_obj.id
                feed.processed = True
                await self.db.commit()
                print(f"[{feed.id}] Feed associato al team '{team_name_ai}'.")
            except Exception as e:
                print(f"[{feed.id}] Errore salvataggio associazione team: {e}")
                await self.db.rollback()
