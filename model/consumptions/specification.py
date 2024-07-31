from sqlalchemy import Column, Integer, ForeignKey, Float, BIGINT
from sqlalchemy.orm import relationship

from model.consumptions.material import Material
from model.database import Base
from model.gypsum_board.GypsumBoard import GypsumBoard


class Specification(Base):
    __tablename__ = 'specifications'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('gypsum_board.id'))
    material_id = Column(Integer, ForeignKey('materials.id'))
    quantity = Column(Float)

    product = relationship('GypsumBoard')
    material = relationship('Material')

    def __init__(self, product: GypsumBoard, material: Material, quantity: float):
        self.product = product
        self.material = material
        self.quantity = quantity

    def __repr__(self):
        return f"{self.id} {self.product} {self.material} {self.quantity}"