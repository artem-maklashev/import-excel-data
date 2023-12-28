from datetime import timedelta
import warnings
from mysql.connector import pooling, IntegrityError

import pandas as pd

import config


class DBProcess:
    connection_pool = pooling.MySQLConnectionPool(
        pool_name="my_pool",
        pool_size=5,
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port
    )

    def clearDB(self):
        cursor = self.connection.cursor()
        query = "DELETE FROM board_defects_log ;"
        # Выполнение SQL-запросов
        cursor.execute(query)

        cursor = self.connection.cursor()
        query = "DELETE FROM plan ;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "DELETE FROM board_production ;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "DELETE FROM productionlog ;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "DELETE FROM delays ;"
        # Выполнение SQL-запросов
        cursor.execute(query)

        query = "ALTER TABLE plan AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "ALTER TABLE board_production AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "ALTER TABLE productionlog AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)

        query = "ALTER TABLE delays AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)

        query = "ALTER TABLE board_defects_log AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def get_connection(self):
        return DBProcess.connection_pool.get_connection()

    def __init__(self, connection):
        self.connection = connection
        print("Подключение к бд установлено")

    def execute_update(self, query, data):
        # Создание курсора
        cursor = self.connection.cursor()
        # Выполнение SQL-запросов
        cursor.execute(query, data)

        # Закрытие курсора и подтверждение транзакции
        self.connection.commit()
        cursor.close()

    def get_gypsum_board(self):
        with self.get_connection():
            query = """SELECT gb.id, 
                        tm.name AS trade_mark, 
                        bt.name AS 'btype',
                        e.name AS edge,
                        l.value AS 'length',
                        t.value AS thickness,
                        w.value AS width
                        FROM gypsum_board gb
                        JOIN trade_mark tm ON gb.trade_mark_id = tm.id
                        JOIN board_types bt ON gb.board_types_id = bt.id
                        JOIN edge e ON gb.edge_id = e.id
                        JOIN thickness t ON gb.thickness_id = t.id
                        JOIN width w ON gb.width_id = w.id
                        JOIN `length` l ON gb.length_id = l.id;"""
            warnings.filterwarnings("ignore")
            df = pd.read_sql(query, self.connection)
            # print(df.dtypes)
        return df

    def create_production_log_record(self, p_date, stop_time, work_time, shift, shift_tag):
        # if shift_tag == 1:
        #     production_start = p_date + timedelta(hours=8)
        if shift_tag == 2:
            production_start = p_date + timedelta(hours=20)
        else:
            production_start = p_date + timedelta(hours=8)
        production_finish = production_start + \
                            timedelta(minutes=(work_time - stop_time))
        production_start_str = production_start.strftime("%Y-%m-%d %H:%M:%S") if production_start else None
        production_finish_str = production_finish.strftime("%Y-%m-%d %H:%M:%S") if production_finish else None
        # if p_date.month == 1:
        #     print(production_start_str, production_finish_str)

        p_date_str = (p_date + timedelta(hours=0)).strftime("%Y-%m-%d") if p_date else None
        shift_id = self.get_shift_id(shift)
        product_types_id = 1
        # print(production_start, production_finish, shift_id, product_types_id)

        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                query = ("INSERT INTO productionlog (production_start, production_finish, shift_id, product_types_id,"
                         " production_date) "
                         "VALUES (%s, %s, %s, %s, %s)")
                # print("Попытка вставить данные:",production_start_str, production_finish_str,
                # shift_id, product_types_id)
                cursor.execute(
                    query, (production_start_str, production_finish_str, shift_id, product_types_id, p_date_str))
                new_id = cursor.lastrowid
                connection.commit()
                production_log_id = new_id
        except IntegrityError as e:
            # Handle duplicate entry
            production_log_id = None
            with self.get_connection() as connection:
                cursor = connection.cursor()
                select_query = ("SELECT id FROM productionlog WHERE production_start = %s AND production_finish = %s "
                                "AND shift_id = %s AND product_types_id = %s;")
                cursor.execute(select_query, (production_start_str,
                                              production_finish_str, shift_id, product_types_id))
                existing_id = cursor.fetchone()
                if existing_id:
                    production_log_id = existing_id[0]
        finally:
            # Ensure proper cleanup
            if cursor:
                cursor.close()

        return production_log_id

    def get_shift_id(self, shift):
        if shift == 'nan':
            return 5
        with self.get_connection() as connection:
            query = "SELECT id FROM shift WHERE name = %s"
            cursor = connection.cursor()
            cursor.execute(query, (shift,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None

    def get_gypsum_board_id(self, trade_mark, btype, edge, thickness, length, width):
        df = self.get_gypsum_board()
        if btype == 'А':
            print("Попался")
        result_series = df.loc[
            (df["trade_mark"] == trade_mark) &
            (df["btype"] == btype) &
            (df["edge"] == edge) &
            (df["thickness"] == thickness) &
            (df["length"] == length) &
            (df["width"] == width), "id"]

        if result_series.empty:
            del df
            print(trade_mark, btype, edge, thickness, width, length)
            return "Not found"
        id_value = int(result_series.iloc[0])
        del df

        return id_value

    def insert_into_board_production(self, production_log_id, board_id, gboard_category, value):
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                query = ("INSERT INTO board_production (production_log_id, gypsum_board_id, gboard_category_id, "
                         "value) VALUES (%s, %s, %s, %s)")
                cursor.execute(query, (production_log_id, board_id, gboard_category, value))
                connection.commit()
            except IntegrityError as e:
                if e.errno == 1062:
                    try:
                        cursor = connection.cursor()
                        query = ("UPDATE board_production SET value = value + %s WHERE production_log_id = %s AND "
                                 "gypsum_board_id = %s AND gboard_category_id = %s ;")
                        cursor.execute(query, (value, production_log_id, board_id, gboard_category))
                        connection.commit()
                    except Exception as update_error:
                        connection.rollback()
                        # Handle the update error, log or re-raise as needed
                        print(f"Update error: {update_error}")
                else:
                    connection.rollback()
                    e.msg = "Ошибка работы с БД"
                    raise e
            finally:
                cursor.close()

    def create_plan_record(self, plan_date, gypsum_board_id, value):
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                query = "INSERT INTO plan (plan_date, gypsum_board_id, value) VALUES (%s, %s, %s);"
                cursor.execute(query, (plan_date, gypsum_board_id, value))
                connection.commit()
        except IntegrityError as e:
            if e.errno == 1062:
                pass
                # print("Duplicate plan entry")
            else:
                e.msg = "Ошибка работы с БД"
                raise e

    def get_unit_part_id(self, division, production_area, unit, unit_part):
        df = self.get_delays()

        if unit_part == " ":
            condition = (
                    (df["division"] == division) &
                    (df["production_area"] == production_area) &
                    (df["unit"] == unit)
            )
        else:
            condition = (
                    (df["division"] == division) &
                    (df["production_area"] == production_area) &
                    (df["unit"] == unit) &
                    (df["unit_part"] == unit_part)
            )
        # print('division', sum(df["division"] == division))
        # print("production_area", sum(df["production_area"] == production_area))
        # print("unit", sum(df["unit"] == unit))
        # print("unit_part", sum(df["unit_part"] == "-"))

        result_series = df.loc[condition, "id"]

        if result_series.empty:
            del df
            print(division, production_area, unit, unit_part)
            return "Not found"

        id_value = int(result_series.iloc[0])
        del df
        # print(id_value)
        return id_value

    def get_delays(self):
        with self.get_connection():
            query = """SELECT 
                        up.id, 
                        d.id as division,
                        pa.name AS production_area,
                        u.name AS unit,
                        up.name AS unit_part
                    FROM unit_part up 
                        JOIN unit u ON up.unit_id = u.id 
                        JOIN production_area pa ON u.production_area_id = pa.id
                        JOIN division d ON d.id = pa.division_id;"""
            warnings.filterwarnings("ignore")
            df = pd.read_sql(query, self.connection)
        return df

    def create_delays_record(self, daley_type_id, date, start_time, end_time, shift_id, unit_part_id, gypsum_board_id):
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                query = ("INSERT INTO delays (delay_type_id, delay_date, start_time, end_time, unit_part_id, shift_id,"
                         " gypsum_board_id) "
                         "VALUES (%s, %s, %s, %s, %s, %s, %s);")
                cursor.execute(query, (daley_type_id, date, start_time, end_time, unit_part_id, shift_id,
                                       gypsum_board_id))
                connection.commit()
        except IntegrityError as e:
            if e.errno == 1062:
                pass
                # print("Duplicate plan entry")
            else:
                e.msg = "Ошибка работы с БД"
                raise e

    def get_daley_type_id(self, delay_type):
        with self.get_connection() as connection:
            query = "SELECT id FROM delay_type WHERE name = %s"
            cursor = connection.cursor()
            cursor.execute(query, (delay_type,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None

    def get_defect_types_id(self, defect_types):
        with self.get_connection() as connection:
            query = "SELECT id FROM defect_types WHERE name = %s"
            cursor = connection.cursor()
            cursor.execute(query, (defect_types,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None

    def get_defect_reason_id(self, defect_reason):
        with self.get_connection() as connection:
            query = "SELECT id FROM defect_reason WHERE name = %s"
            cursor = connection.cursor()
            cursor.execute(query, (defect_reason,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None

    def get_defect_id(self, defect_types_id, defect_reason_id, defect_name):
        with self.get_connection() as connection:
            query = "SELECT id FROM defects WHERE name = %s  AND defect_types_id = %s and defect_reason_id = %s"
            cursor = connection.cursor()
            values = (defect_name, defect_types_id, defect_reason_id)
            cursor.execute(query, values)
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                print(defect_types_id, defect_reason_id, defect_name)
                return None

    def get_production_log_id(self, defect_date, shift_id):
        with self.get_connection() as connection:
            query = "SELECT id FROM productionlog WHERE production_date = %s AND shift_id = %s"
            cursor = connection.cursor()
            values = (defect_date, shift_id)
            cursor.execute(query, values)
            production_log_ids = []
            while True:
                result = cursor.fetchone()
                if not result:
                    break
                production_log_ids.append(result[0])
            if len(production_log_ids) == 0:
                print("Not found production_log_id. Date ", defect_date, " shift ", shift_id)
            return production_log_ids

    def get_board_production_id(self, production_log_ids: list, gypsum_board_id):
        # production_log_ids_str = ",".join(map(str, production_log_ids))
        # print(production_log_ids)
        with self.get_connection() as connection:
            # query = ("SELECT DISTINCT production_log_id FROM board_production "
            #          "WHERE production_log_id IN (%s) "
            #          "AND gypsum_board_id = %s")
            # cursor = connection.cursor()
            # values = (production_log_ids_str, gypsum_board_id)
            # cursor.execute(query, values)
            # result = cursor.fetchone()
            # if result:
            #     return result[0]
            # else:
            #     print("Не нашел production_log_id", values)
            #     return None
            for ids in production_log_ids:
                query = ("SELECT DISTINCT production_log_id FROM board_production "
                         "WHERE production_log_id = %s "
                         "AND gypsum_board_id = %s")
                cursor = connection.cursor()
                values = (ids, gypsum_board_id)
                cursor.execute(query, values)
                result = cursor.fetchone()
                if result:
                    return result[0]
            print("Не нашел ", production_log_ids, " гипсокартон ", gypsum_board_id)
        return None

    def create_board_defects_record(self, board_production_id, defect_value, defect_id):
        with self.get_connection() as connection:
            query = ("SELECT id FROM board_defects_log WHERE production_log_id = %s AND value = %s "
                     "AND defects_id = %s ")
            cursor = connection.cursor()
            values = (board_production_id, defect_value, defect_id)
            cursor.execute(query, values)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return
            else:
                cursor = connection.cursor()
                query = ("INSERT INTO board_defects_log (production_log_id, value, defects_id) "
                         "VALUES (%s, %s, %s);")
                cursor.execute(query, (board_production_id, defect_value, defect_id))
                cursor.close()
                connection.commit()
