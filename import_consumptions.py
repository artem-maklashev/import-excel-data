import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload

from model.database import Base
from model.gypsum_board.BoardProduction import BoardProduction
import config

class ImportConsumption:
    local_tz = pytz.timezone(config.timezone)

    # Создание движка SQLAlchemy и сессии
    engine = create_engine(f'mysql+mysqlconnector://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}', echo=True)
    Session = sessionmaker(bind=engine)

    @classmethod
    def initialize_database(cls):
        # Создание всех таблиц
        Base.metadata.create_all(cls.engine)

    @classmethod
    def get_board_productions(cls):
        session = cls.Session()
        try:
            # Выполнение запроса с использованием SQLAlchemy ORM и жадной загрузки связанных объектов
            objects = session.query(BoardProduction).options(
                joinedload(BoardProduction.production_log),
                joinedload(BoardProduction.gypsum_board),
                joinedload(BoardProduction.gboard_category)
            ).all()
            for obj in objects[:5]:
                print(obj)
        finally:
            session.close()  # Обязательно закрываем сессию

        return objects

if __name__ == "__main__":
    # Инициализация базы данных (создание таблиц, если это необходимо)
    ImportConsumption.initialize_database()

    # Получение данных
    objects = ImportConsumption.get_board_productions()

    # Закрываем сессию и работаем с данными
    # for obj in objects[:5]:
    #     print(obj)
