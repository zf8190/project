from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.models.base import Base

class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    logo_url = Column(String(255), nullable=True)

    # Relazione 1:1 con Article
    article = relationship("Article", back_populates="team", uselist=False)

    # Relazione one-to-many con Feed (lista di Feed)
    feeds = relationship("Feed", back_populates="team", cascade="all, delete-orphan")
