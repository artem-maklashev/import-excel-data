from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from model.database import Base
from model.gypsum_board.BoardTypes import BoardTypes
from model.gypsum_board.Edge import Edge
from model.gypsum_board.Length import Length
from model.gypsum_board.ProductTypes import ProductTypes
from model.gypsum_board.Thickness import Thickness
from model.gypsum_board.Trademark import TradeMark
from model.gypsum_board.Width import Width


class GypsumBoard(Base):
    __tablename__ = "gypsum_board"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_mark_id = Column(Integer, ForeignKey('trade_mark.id'))
    board_types_id = Column(Integer, ForeignKey('board_types.id'))
    edge_id = Column(Integer, ForeignKey('edge.id'))
    thickness_id = Column(Integer, ForeignKey('thickness.id'))
    width_id = Column(Integer, ForeignKey('width.id'))
    length_id = Column(Integer, ForeignKey('length.id'))
    types_id = Column(Integer, ForeignKey('product_types.id'))

    trade_mark = relationship('TradeMark')
    board_types = relationship('BoardTypes')
    edge = relationship('Edge')
    thickness = relationship('Thickness')
    width = relationship('Width')
    length = relationship('Length')
    types = relationship('ProductTypes')



    def __init__(self, trade_mark: TradeMark, board_types: BoardTypes, edge: Edge,
                 thickness: Thickness, width: Width, length: Length, types: ProductTypes):
        self.id = id
        self.trade_mark = trade_mark
        self.board_types = board_types
        self.edge = edge
        self.thickness = thickness
        self.width = width
        self.length = length
        self.types = types

    def __repr__(self):
        return (f"{self.types} {self.trade_mark} {self.board_types}-{self.edge} {self.length}-"
                f"{self.width}-{self.thickness}")