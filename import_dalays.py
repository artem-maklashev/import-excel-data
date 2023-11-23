import mysql.connector

from db_process import DBProcess
from excel_delays import ExcelDelays


def main():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="402986",
        database="goldengroup",
        port=3306)
    db_process = DBProcess(connection)
    path: str = (r"F:\YandexDisk-ar.maclashev\Обучение Python\ДИПЛОМ\Начальные данные\простои.xlsx"
                 )
    excel_processor = ExcelDelays(path)
    excel_processor.import_delays_data(db_process)


if __name__ == "__main__":
    main()