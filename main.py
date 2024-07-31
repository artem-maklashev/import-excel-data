import threading
import time

import mysql.connector
import win32com

import config
import import_dalays
from db_process import DBProcess
from excel_process import Excel
from import_consumptions import ImportConsumption
from import_defects import ImportDefects
from win32com import client


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
    select_production = (excel_data["plan"].isna())
    excel_processor.import_production_data(select_production, db_process)
    select_plan = (excel_data["plan"] > 0)
    excel_processor.import_production_data(select_plan, db_process)
    errors_list = excel_processor.errors_list
    print("Ошибок: ", len(errors_list))
    if len(errors_list) != 0:
        for i in range(0, 10):
            print(errors_list[i])
    defects = ImportDefects()
    defects.import_defects_data()
    # import_consumptions = ImportConsumption()
    # import_consumptions.initialize_database()
    # import_consumptions.get_board_productions()


xl_apps = [config.production, config.delays, config.defects, config.consumptions]
xl_app = win32com.client.DispatchEx("Excel.Application")
for book in xl_apps:
    wb = xl_app.Workbooks.Open(book)
    wb.RefreshAll()
    print("обновляем книгу", book)
    xl_app.CalculateUntilAsyncQueriesDone()
    # time.sleep(10)
    wb.Save()
    wb.Close(SaveChanges=True)
xl_app.Quit()

thread1 = threading.Thread(main())
thread2 = threading.Thread(import_dalays.main())

thread1.start()
thread2.start()

# thread1.join()
# thread2.join()
# if __name__ == "__main__":
#     main()
