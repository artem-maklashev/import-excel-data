from string import ascii_uppercase

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from model.database import Base


class Thickness(Base):
    __tablename__ = "thickness"

    id = Column(Integer, primary_key=True, autoincrement=True)
    value = Column(String(255))

    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"{self.value}"