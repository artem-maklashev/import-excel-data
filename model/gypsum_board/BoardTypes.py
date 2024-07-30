from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from model.database import Base


class BoardTypes(Base):
    __tablename__ = "board_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))
    description = Column(String(255))

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    def __repr__(self):
        return f"{self.name}"