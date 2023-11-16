import mysql.connector

from db_process import DBProcess
from excel_process import Excel


def main():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="402986",
        database="goldengroup",
        port=3306)
    db_process = DBProcess(connection)
    df = db_process.get_gypsum_board()
    print(df.head())
    path: str = r"D:\YandexDisk\Обучение Python\ДИПЛОМ\Начальные данные\выпуск.xlsx"
    print(path)
    excel_processor = Excel(path)
    excel_data = excel_processor.df
    condition = (excel_data["plan"].isna()) & (excel_data["1/2"] > 0)
    excel_production_data = excel_processor.import_production_data(condition, db_process)
    errors_list = excel_processor.errors_list
    print("Ошибок: ", len(errors_list))
    if len(errors_list) != 0:
        for i in range(0, 10):
            print(errors_list[i])


if __name__ == "__main__":
    main()

