from sqlalchemy import Column, Integer, ForeignKey, ForeignKeyConstraint, Double, BIGINT

from sqlalchemy.orm import relationship

from model.ProductionList import ProductionList
from model.consumptions.material import Material
from model.database import Base


class Consumption(Base):
    __tablename__ = 'material_consumption'

    id = Column(Integer, primary_key=True, autoincrement=True)
    production_list_id = Column(BIGINT, ForeignKey('productionlog.id'))
    material_id = Column(BIGINT, ForeignKey('materials.id'))
    quantity = Column(Double)

    production_list = relationship('ProductionList')
    material = relationship('Material')

    def __init__(self, production_list: ProductionList, material: Material, quantity: float):
        self.production_list = production_list
        self.material = material
        self.quantity = quantity

    def __repr__(self):
        return f"{self.id} {self.production_list} {self.material} {self.quantity}"