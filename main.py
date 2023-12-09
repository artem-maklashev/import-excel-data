import threading

import mysql.connector

import config
import import_dalays
from db_process import DBProcess
from excel_process import Excel


def main():
    connection = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port)
    db_process = DBProcess(connection)
    df = db_process.get_gypsum_board()
    print(df.head())
    path: str = config.production
    db_process.clearDB()
    print(path)
    excel_processor = Excel(path)
    excel_data = excel_processor.df
    select_production = (excel_data["plan"].isna()) & (excel_data["1/2"] > 0)
    excel_production_data = excel_processor.import_production_data(select_production, db_process)
    select_plan = (excel_data["plan"] > 0)
    excel_processor.import_production_data(select_plan, db_process)
    errors_list = excel_processor.errors_list
    print("Ошибок: ", len(errors_list))
    if len(errors_list) != 0:
        for i in range(0, 10):
            print(errors_list[i])


thread1 = threading.Thread(main())
thread2 = threading.Thread(import_dalays.main())

thread1.start()
thread2.start()
# if __name__ == "__main__":
#     main()
