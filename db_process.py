from datetime import timedelta
import warnings
from mysql.connector import pooling, IntegrityError

import pandas as pd


class DBProcess:
    connection_pool = pooling.MySQLConnectionPool(
        pool_name="my_pool",
        pool_size=5,
        host="localhost",
        user="root",
        password="402986",
        database="goldengroup",
        port=3306
    )
    def clearDB(self):
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

        query = "ALTER TABLE plan AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "ALTER TABLE board_production AUTO_INCREMENT=1;"
        # Выполнение SQL-запросов
        cursor.execute(query)
        query = "ALTER TABLE productionlog AUTO_INCREMENT=1;"
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
        return df

    def create_production_log_record(self, p_date, stop_time, work_time, shift, shift_tag):
        if shift_tag == 1:
            production_start = p_date + timedelta(hours=8)
        elif shift_tag == 2:
            production_start = p_date + timedelta(hours=20)
        production_finish = production_start + \
                            timedelta(minutes=(work_time - stop_time))
        production_start_str = production_start.strftime("%Y-%m-%d %H:%M:%S") if production_start else None
        production_finish_str = production_finish.strftime("%Y-%m-%d %H:%M:%S") if production_finish else None
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
        print(trade_mark, btype, edge, thickness, length, width)
        result_series = df.loc[
            (df["trade_mark"] == trade_mark) &
            (df["btype"] == btype) &
            (df["edge"] == edge) &
            (df["thickness"] == thickness) &
            (df["length"] == length) &
            (df["width"] == width),"id"]

        if result_series.empty:
            del df
            return "Not found"
        id_value = int(result_series.iloc[0])
        del df

        return id_value

    def insert_into_board_production(self, production_log_id, board_id, gboard_category, value):
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                query = ("INSERT INTO board_production (production_log_id, gypsum_board_id, gboard_category_id, "
                         "value) VALUES (%s, %s, %s, %s)")
                cursor.execute(
                    query, (production_log_id, board_id, gboard_category, value))
                connection.commit()
        except IntegrityError as e:
            if e.errno == 1062:
                pass
                # print("Duplicate entry")
            else:
                e.msg = "Ошибка работы с БД"
                raise e

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
        print(df.loc[df["id"] == 55])
        print(division, production_area, unit, unit_part)
        if unit_part == " ":
            condition = (
                    (df["division"] == division) &
                    (df["production_area"] == production_area) &
                    (df["unit"] == unit) &
                    (df["unit_part"] == "-")
            )
        else:
            condition = (
                    (df["division"] == division) &
                    (df["production_area"] == production_area) &
                    (df["unit"] == unit) &
                    (df["unit_part"] == unit_part)
            )

        result_series = df.loc[condition, "id"]

        if result_series.empty:
            del df
            return "Not found"

        id_value = int(result_series.iloc[0])
        del df
        print(id_value)
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
                query = ("INSERT INTO dalays (daley_type_id, dalay_date, start_time, end_time, unit_part_id, shift_id,"
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
