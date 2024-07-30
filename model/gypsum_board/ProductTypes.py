from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from model.database import Base


class ProductTypes(Base):
    __tablename__ = "product_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))

    def __init__(self,  name: str):
        self.name = name

    def __repr__(self):
        return f"{self.name}"