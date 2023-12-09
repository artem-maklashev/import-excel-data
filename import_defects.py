import mysql.connector
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

import config
from db_process import DBProcess


class ImportDefects:
    errors_list = []

    def read_data(self):
        path: str = config.defects
        data: DataFrame = pd.read_excel(path)
        return data

    def import_defects_data(self, data):
        connection = mysql.connector.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database,
            port=config.port)
        db_processor = DBProcess(connection)
        with db_processor.get_connection() as connection:
            df = data
            for row in tqdm(df.itertuples(), total=df.shape[0], desc="Обработка", colour="green"):
                defect_date = row.date
                defect_shift = row.shift
                defect_trade_mark = row.trade_mark
                defect_board_type = row.board_type
                defect_edge = row.edge
                defect_length = row.length
                defect_width = row.width
                defect_thickness = row.thickness
                defect_types = row.defect_types
                defect_reason = row.defect_reason
                defect_name = row.defect_name
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
                        + defect_trade_mark
                        + " "
                        + defect_board_type
                        + " "
                        + defect_edge
                        + " "
                        + defect_thickness
                        + " "
                        + defect_length
                    )

                db_processor.create_delays_record(delay_type_id, date, start_time, end_time, shift_id, unit_part_id,
                                                  board_id)
                time.sleep(0.001)
