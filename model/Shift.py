from sqlalchemy import Column, Integer, String

from model.database import Base


class Shift(Base):
    __tablename__ = "shift"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))

    def __init__(self,  name: str):
        self.name = name

    def __repr__(self):
        return f"{self.name}"