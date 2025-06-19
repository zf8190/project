from sqlalchemy import Column, Integer, String, Text, Boolean, TIMESTAMP, ForeignKey
from sqlalchemy.sql import func
from app.models.base import Base
from sqlalchemy.orm import relationship

class Feed(Base):
    __tablename__ = "feeds"

    id = Column(Integer, primary_key=True, index=True)
    feed_source = Column(String(100), nullable=False)
    feed_entry_id = Column(String(1024), unique=True, nullable=False)
    title = Column(String(1024), nullable=False)
    link = Column(String(1024), nullable=False)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    published_at = Column(TIMESTAMP(timezone=True), nullable=False)
    processed = Column(Boolean, default=False, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)  # FK verso Team
    team = relationship("Team", back_populates="feeds")
