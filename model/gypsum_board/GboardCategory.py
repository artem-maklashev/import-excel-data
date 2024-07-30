from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from model.database import Base


class GboardCategory(Base):
    __tablename__ = "gboard_category"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255))

    def __init__(self,  title: str):
        self.title = title

    def __repr__(self):
        return f"{self.title}"