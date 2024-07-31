from datetime import datetime
from pydoc import describe
from typing import List

import pandas as pd
import pytz
from colorama import colorama_text
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, joinedload
from tqdm import tqdm

from model.consumptions.consumption import Consumption
from model.consumptions.material import Material
from model.database import Base
from model.excel_consumption import read_consumption, select_by_date_from_excel
from model.gypsum_board.BoardProduction import BoardProduction
import config





class ImportConsumption:
    local_tz = pytz.timezone(config.timezone)

    # Создание движка SQLAlchemy и сессии
    engine = create_engine(f'mysql+mysqlconnector://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}')
    Session = sessionmaker(bind=engine)


    @classmethod
    def initialize_database(cls):
        # Создание всех таблиц
        Base.metadata.create_all(cls.engine)

    @classmethod
    def get_board_productions(cls):
        session = cls.Session()

        try:
            session.query(Consumption).delete()
            session.execute(text("ALTER TABLE material_consumption AUTO_INCREMENT = 1"))
            session.commit()
            # Выполнение запроса с использованием SQLAlchemy ORM и жадной загрузки связанных объектов
            objects = session.query(BoardProduction).options(
                joinedload(BoardProduction.production_log),
                joinedload(BoardProduction.gypsum_board),
                joinedload(BoardProduction.gboard_category)
            ).all()
            for obj in objects[-10:]:
                print(obj)
            excel_consumption: pd.DataFrame = read_consumption()
            excel_consumption[['length', 'width']] = excel_consumption[['length','width']].astype(str)


            for obj in tqdm(objects, total=len(objects), desc="Расход материалов", colour='MAGENTA'):
                data_to_import = select_by_date_from_excel(obj, excel_consumption, session)
                if data_to_import:
                    session.add_all(data_to_import)
            session.commit()
        finally:
            session.close()  # Обязательно закрываем сессию

        return objects

def main():
    ImportConsumption.initialize_database()
    ImportConsumption.get_board_productions()

if __name__ == "__main__":
    # Инициализация базы данных (создание таблиц, если это необходимо)
    # ImportConsumption.initialize_database()
    main()
    # Получение данных
    # objects = ImportConsumption.get_board_productions()

    # Закрываем сессию и работаем с данными
    # for obj in objects[:5]:
    #     print(obj)
