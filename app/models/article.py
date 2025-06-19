from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.models.base import Base

class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), unique=True)  # ← questa parte è cruciale!
    title = Column(String(255), nullable=False)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    sources = Column(Text, nullable=True)  # Fonti consultate, testo libero o JSON string
    last_updated = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    team = relationship("Team", back_populates="article")
    
