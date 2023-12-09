import mysql.connector

import config
from db_process import DBProcess
from excel_delays import ExcelDelays


def main():
    connection = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port)
    db_process = DBProcess(connection)
    path: str = config.delays
    excel_processor = ExcelDelays(path)
    excel_processor.import_delays_data(db_process)


# if __name__ == "__main__":
#     main()

