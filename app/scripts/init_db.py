import os
from sqlalchemy.orm import sessionmaker
from app.models.team import Team
from app.models.article import Article
from app.models.article_history import ArticleHistory
from app.db import get_engine
from app.models import Base
from dotenv import load_dotenv

load_dotenv()

def main():
    engine = get_engine()
    # Crea tutte le tabelle se non esistono
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    STATIC_URL = os.getenv("STATIC_URL")
    if not STATIC_URL:
        raise ValueError("STATIC_URL non impostato nelle variabili ambiente")

    # Lista squadre con solo nomi; i loghi li costruiamo usando STATIC_URL
    teams = [
        "Napoli",
        "Inter",
        "Atalanta",
        "Juventus",
        "Roma",
        "Fiorentina",
        "Lazio",
        "Bologna",
        "Milan",
        "Como",
    ]

    for team_name in teams:
        existing = session.query(Team).filter_by(name=team_name).first()
        if not existing:
            logo_url = f"{STATIC_URL}/logos/{team_name.lower()}.png"  # es: https://.../logos/napoli.png
            team_obj = Team(name=team_name, logo_url=logo_url)
            session.add(team_obj)

    session.commit()
    print("Database initialized and teams seeded.")

if __name__ == "__main__":
    main()
