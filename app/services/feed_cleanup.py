import datetime
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.feed import Feed

async def sgr_ezza_feeds(db: AsyncSession) -> int:
    """
    Aggiorna a processed=True tutti i feed non processati con published_at più vecchio di 24 ore.

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
        feed.processed = True
        count += 1

    if count > 0:
        await db.commit()

    return count
