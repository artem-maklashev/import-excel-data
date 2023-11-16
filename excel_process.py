import pandas as pd

from db_process import DBProcess


class Excel:

    def __init__(self, path: str):
        self.df = pd.read_excel(path)
        self.errors_list = []

    def import_production_data(self, condition, db_processor: DBProcess):
        with db_processor.get_connection() as connection:
            df = self.df.loc[condition]
            df = df.rename(columns={"1/2": "am_pm"})
            for row in df.itertuples():
                date = row.p_date
                shift_tag = row.am_pm
                shift = row.shift
                trade_mark = row.trade_mark
                btype = row.type
                edge = row.edge
                length = str(int(row.lengh))
                thickness = str(row.thickness)
                if length == "2398":
                    width = "1194"
                else:
                    width = "1200"
                stop_time = row.stop_time if pd.notna(row.stop_time) else 0
                work_time = row.work_time   if pd.notna(row.work_time) else 0
                total = row.forming
                valid = row.good_quality
                tech_sheet = row.tech_sheet
                conditionaly_valid = row.category_2
                defects_total = row.category_a
                uncertain_quality = row.uncertain_quality
                wet_reject = row.wet_reject
                laboratory_reject = row.laboratory_reject
                cut_reject = row.cut_reject
                dry_laboratory = row.dry_laboratory

                board_id = db_processor.get_gypsum_board_id(
                    trade_mark, btype, edge, thickness, length, width
                )
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
                data_to_insert = [
                    (total, 1),
                    (valid, 2),
                    (conditionaly_valid, 3),
                    (tech_sheet, 4),
                    (uncertain_quality, 5),
                    (defects_total, 6),
                    (wet_reject, 7),
                    (laboratory_reject, 8),
                    (cut_reject, 9),
                    (dry_laboratory, 10)
                ]

                production_log_id = db_processor.create_production_log_record(date, stop_time, work_time, shift, shift_tag)

                for data, category_id in data_to_insert:
                    if pd.notna(data):
                        db_processor.insert_into_board_production(production_log_id, board_id, category_id, data)

