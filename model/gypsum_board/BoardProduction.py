from sqlalchemy import Column, Integer, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from model.database import Base
from model.ProductionList import ProductionList
from model.gypsum_board.GboardCategory import GboardCategory
from model.gypsum_board.GypsumBoard import GypsumBoard

class BoardProduction(Base):
    __tablename__ = 'board_production'

    id = Column(Integer, primary_key=True, autoincrement=True)
    production_log_id = Column(Integer, ForeignKey('productionlog.id'), nullable=False)
    gypsum_board_id = Column(Integer, ForeignKey('gypsum_board.id'), nullable=False)
    gboard_category_id = Column(Integer, ForeignKey('gboard_category.id'), nullable=False)
    value = Column(Float, nullable=True)  # float соответствует типу float в MySQL

    # Установление отношений
    production_log = relationship('ProductionList', backref='board_productions')  # связь с ProductionList
    gypsum_board = relationship('GypsumBoard', backref='board_productions')       # связь с GypsumBoard
    gboard_category = relationship('GboardCategory', backref='board_productions')  # связь с GboardCategory

    __table_args__ = (
        # Уникальные ограничения
        UniqueConstraint('production_log_id', 'gypsum_board_id', 'gboard_category_id', name='prodaction_unique'),
    )

    def __init__(self, production_log: ProductionList, gypsum_board: GypsumBoard, gboard_category: GboardCategory, value: float):
        self.production_log = production_log
        self.gypsum_board = gypsum_board
        self.gboard_category = gboard_category
        self.value = value

    def __repr__(self):
        production_log_id = self.production_log.id if self.production_log else 'None'
        gypsum_board_id = self.gypsum_board if self.gypsum_board else 'None'
        gboard_category_title = self.gboard_category.title if self.gboard_category else 'None'
        return (f"id={self.id}, production_log_id='{production_log_id}', "
                f"gypsum_board='{gypsum_board_id}', '{gboard_category_title}', value={self.value}")



