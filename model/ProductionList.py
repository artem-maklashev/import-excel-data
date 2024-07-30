from datetime import datetime
from sqlalchemy import Column, Integer, DATETIME, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from model.Shift import Shift
from model.database import Base
from model.gypsum_board.ProductTypes import ProductTypes



class ProductionList(Base):
    __tablename__ = "productionlog"
    id = Column(Integer, primary_key=True, autoincrement=True)
    production_start = Column(DateTime, default=datetime.now())
    production_finish = Column(DateTime, default=datetime.now())
    shift_id = Column(Integer, ForeignKey("shift.id"))
    product_types_id =Column(Integer, ForeignKey("product_types.id"))
    production_date = Column(DateTime, default=datetime.now())

    shift = relationship('Shift')
    product_types = relationship('ProductTypes')

    def __init__(self, production_start: datetime, production_finish: datetime, shift: Shift,
                 product_types: ProductTypes, production_date: datetime):
        self.id = id
        self.production_start = production_start
        self.production_finish = production_finish
        self.shift = shift
        self.product_types = product_types
        self.production_date = production_date