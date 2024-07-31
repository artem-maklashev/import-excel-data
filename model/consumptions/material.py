from sqlalchemy import Column, Integer, String, BIGINT

from model.database import Base


class Material(Base):
    __tablename__ = 'materials'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"{self.name}"