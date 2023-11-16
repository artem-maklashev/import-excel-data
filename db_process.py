from datetime import timedelta
import warnings
import mysql.connector
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
                        bt.name AS 'type',
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
        shift_id = self.get_shift_id(shift)
        product_types_id = 1
        print(production_start, production_finish, shift_id, product_types_id)

        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                query = ("INSERT INTO productionlog (production_start, production_finish, shift_id, product_types_id) "
                         "VALUES (%s, %s, %s, %s)")
                cursor.execute(
                    query, (production_start_str, production_finish_str, shift_id, product_types_id))
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

    def get_gypsum_board_id(self, trade_mark, type, edge, thickness, length, width):
        df = self.get_gypsum_board()
        result_series = df.loc[
            (df["trade_mark"] == trade_mark) &
            (df["type"] == type) &
            (df["edge"] == edge) &
            (df["thickness"] == thickness) &
            (df["length"] == length) &
            (df["width"] == width),
            "id"
        ]
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
                print("Duplicate entry")
            else:
                e.msg = "Ошибка работы с БД"
                raise e
