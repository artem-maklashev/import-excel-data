import time

import pandas as pd
import pytz
from tqdm import tqdm_notebook, tqdm

import config
from db_process import DBProcess


class ExcelDelays:
    local_tz = pytz.timezone(config.timezone)
    def __init__(self, path: str):
        self.df = pd.read_excel(path)
        self.errors_list = []

    def import_delays_data(self, db_processor: DBProcess):
        with db_processor.get_connection() as connection:
            df = self.df

            for row in tqdm(df.itertuples(), total=df.shape[0], desc="Обработка таблицы простоев", colour="blue"):
                delay_type = row.delay_type
                shift = row.shift
                trade_mark = row.board_trade_mark
                btype = row.board_type
                edge = row.edge
                length = str(int(row.length))
                thickness = str(row.thickness)
                if length == "2398":
                    width = "1194"
                else:
                    width = "1200"
                date = row.delay_date
                # Установите часовой пояс для start_time и end_time
                if pd.notna(row.start_time):
                    start_time = self.local_tz.localize(row.start_time).astimezone(pytz.utc)
                    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                else:
                    start_time_str = "0"

                if pd.notna(row.end_time):
                    end_time = self.local_tz.localize(row.end_time).astimezone(pytz.utc)
                    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S %Z%z")
                else:
                    end_time_str = "0"
                # start_time = row.start_time.strftime("%Y-%m-%d %H:%M:%S %Z%z") if pd.notna(row.start_time) else 0
                # end_time = row.end_time.strftime("%Y-%m-%d %H:%M:%S %Z%z") if pd.notna(row.end_time) else 0
                division = 1
                production_area = row.production_area
                unit = row.unit
                unit_part = row.unit_part if pd.notna(row.unit_part) else " "

                board_id = db_processor.get_gypsum_board_id(
                    trade_mark, btype, edge, thickness, length, width)
                shift_id = db_processor.get_shift_id(shift)
                unit_part_id = db_processor.get_unit_part_id(division, production_area, unit, unit_part)
                delay_type_id = db_processor.get_daley_type_id(delay_type)
                if board_id == "Not found":
                    self.errors_list.append(
                        str(date)
                        + " "
                        + trade_mark
                        + " "
                        + btype
                        + " "
                        + edge
                        + " "
                        + thickness
                        + " "
                        + length
                    )

                db_processor.create_delays_record(delay_type_id, date, start_time, end_time, shift_id, unit_part_id,
                                                  board_id)
                time.sleep(0.001)
