# app/services/team_service.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete, func
from app.models.team import Team

async def get_all_teams(db: AsyncSession):
    result = await db.execute(select(Team).order_by(Team.id))
    return result.scalars().all()

async def get_team_by_name(db: AsyncSession, name: str):
    result = await db.execute(
        select(Team).filter(Team.name.ilike(name))
    )
    return result.scalars().first()

async def update_teams_list(db: AsyncSession, new_teams: list):
    # Elimina tutte le squadre esistenti
    await db.execute(delete(Team))
    await db.commit()

    # Inserisce le nuove squadre
    for team_data in new_teams:
        team = Team(name=team_data['name'], logo_url=team_data['logo_url'])
        db.add(team)
    await db.commit()

async def team_exists(db: AsyncSession, name: str) -> bool:
    result = await db.execute(
        select(func.count()).select_from(Team).filter(Team.name.ilike(name))
    )
    count = result.scalar_one()
    return count > 0
