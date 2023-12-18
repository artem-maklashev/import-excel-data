import mysql.connector
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm
import time
import config
from db_process import DBProcess


def read_data():
    path: str = config.defects
    data: DataFrame = pd.read_excel(path)
    return data


class ImportDefects:
    errors_list = []
    data = read_data()

    def import_defects_data(self):
        connection = mysql.connector.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database,
            port=config.port)
        db_processor = DBProcess(connection)
        with db_processor.get_connection() as connection:
            df = self.data
            for row in tqdm(df.itertuples(), total=df.shape[0], desc="Обработка таблицы брака", colour="green"):
                defect_date = row.date
                defect_shift = row.shift
                defect_trade_mark = row.trade_mark
                defect_board_type = row.board_type
                defect_edge = row.edge
                defect_length = str(row.length)
                defect_width = str(row.width)
                defect_thickness = row.thickness
                defect_types = row.defect_types
                defect_reason = row.defect_reason
                defect_name = row.name
                defect_value = row.value
                shift_id = db_processor.get_shift_id(defect_shift)
                gypsum_board_id = db_processor.get_gypsum_board_id(defect_trade_mark, defect_board_type, defect_edge,
                                                                   defect_thickness, defect_length, defect_width)
                defect_types_id = db_processor.get_defect_types_id(defect_types)
                defect_reason_id = db_processor.get_defect_reason_id(defect_reason)
                defect_id = db_processor.get_defect_id(defect_types_id, defect_reason_id, defect_name)
                production_log_id = db_processor.get_production_log_id(defect_date, shift_id)
                board_production_id = db_processor.get_board_production_id(production_log_id, gypsum_board_id)
                if gypsum_board_id == "Not found":
                    self.errors_list.append(
                        str(defect_date)
                        + " "
                        + str(defect_trade_mark)
                        + " "
                        + str(defect_board_type)
                        + " "
                        + str(defect_edge)
                        + " "
                        + str(defect_thickness)
                        + " "
                        + str(defect_length)
                    )

                db_processor.create_board_defects_record(board_production_id, defect_value, defect_id)
                time.sleep(0.001)
