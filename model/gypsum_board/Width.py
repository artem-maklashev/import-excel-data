from sqlalchemy import Column, Integer, String

from model.database import Base


class Width(Base):
    __tablename__ = "width"

    id = Column(Integer, primary_key=True, autoincrement=True)
    value = Column(String(255))

    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"{self.value}"